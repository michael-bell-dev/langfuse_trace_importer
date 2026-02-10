#!/usr/bin/env python3
"""
Langfuse Trace Importer with Agent Squad Support

This script reads an exported Langfuse trace JSON file and creates:
1. A main trace with all observations
2. Separate traces for each agent in a squad (from first message to handoff call)
"""

import json
import argparse
import sys
from datetime import datetime
from typing import List, Dict, Any, Optional
import uuid
import os
import requests
from dotenv import load_dotenv
from collections.abc import Mapping

# --- LOAD .env ---
load_dotenv()


def load_trace_file(filepath: str) -> List[Dict[str, Any]]:
    encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']
    
    for encoding in encodings:
        try:
            with open(filepath, 'r', encoding=encoding) as f:
                data = json.load(f)
            if encoding != 'utf-8':
                print(f"Note: File was read using {encoding} encoding")
            return data
        except UnicodeDecodeError:
            continue
        except FileNotFoundError:
            print(f"Error: File '{filepath}' not found")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON in file: {e}")
            sys.exit(1)
    
    print("Error: Could not decode file with any common encoding")
    sys.exit(1)


def normalize_tool_call_keys(value: Any) -> Any:
    if isinstance(value, str):
        return (
            value
            .replace("toolCallId", "tool_call_id")
            .replace("toolCalls", "tool_calls")
            .replace("toolCall", "tool_call")
        )
    if isinstance(value, dict):
        return {k: normalize_tool_call_keys(v) for k, v in value.items()}
    if isinstance(value, list):
        return [normalize_tool_call_keys(v) for v in value]
    return value


def deep_merge(a: Any, b: Any) -> Any:
    """Recursively merge b into a"""
    if isinstance(a, Mapping) and isinstance(b, Mapping):
        result = dict(a)
        for k, v in b.items():
            if k in result:
                result[k] = deep_merge(result[k], v)
            else:
                result[k] = v
        return result
    if isinstance(a, list) and isinstance(b, list):
        return a + b
    return b  # fallback: overwrite


def collect_trace_io(observations: List[Dict[str, Any]]) -> (Any, Any):
    merged_input = {}
    merged_output = {}
    for obs in observations:
        if obs.get('input') is not None:
            merged_input = deep_merge(merged_input, obs['input'])
        if obs.get('output') is not None:
            merged_output = deep_merge(merged_output, obs['output'])
    return normalize_tool_call_keys(merged_input), normalize_tool_call_keys(merged_output)


def is_handoff_tool_call(obs: Dict[str, Any]) -> bool:
    """Check if an observation contains a handoff or endCall tool call"""
    output = obs.get('output')
    if not output:
        return False
    
    # Handle string output (JSON)
    if isinstance(output, str):
        try:
            output = json.loads(output)
        except:
            return False
    
    # Check if it's a tool_calls type output
    if isinstance(output, dict) and output.get('type') == 'tool_calls':
        tool_calls = output.get('output', [])
        for tc in tool_calls:
            func_name = tc.get('function', {}).get('name', '')
            # Check for handoff or endCall
            if 'handoff' in func_name.lower() or 'endcall' in func_name.lower():
                return True
    
    return False


def is_handoff_result(obs: Dict[str, Any]) -> bool:
    """Check if an observation is a result of a handoff or endCall tool call"""
    # Check the name field for handoff/endCall references
    name = obs.get('name', '')
    if 'handoff' in name.lower() or 'endcall' in name.lower():
        return True
    
    # Check input for tool calls
    input_data = obs.get('input')
    if isinstance(input_data, str):
        try:
            input_data = json.loads(input_data)
        except:
            pass
    
    if isinstance(input_data, dict):
        tool_calls = input_data.get('toolCalls', [])
        for tc in tool_calls:
            tool_call = tc.get('toolCall', {})
            func_name = tool_call.get('name', '')
            if 'handoff' in func_name.lower() or 'endcall' in func_name.lower():
                return True
    
    # Check output for handoff/endCall results
    output = obs.get('output')
    if isinstance(output, str):
        try:
            output = json.loads(output)
        except:
            pass
    
    if isinstance(output, list):
        for item in output:
            if isinstance(item, dict):
                name = item.get('name', '')
                if 'handoff' in name.lower() or 'endcall' in name.lower():
                    return True
    
    return False


def extract_agent_segments(observations: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """
    Extract separate agent segments from observations.
    Each segment starts after a handoff completes and ends before the next handoff starts.
    Excludes all handoff/endCall related observations.
    """
    segments = []
    current_segment = []
    skip_until_new_agent = False
    
    for i, obs in enumerate(observations):
        # Check if this is a handoff/endCall tool call
        if is_handoff_tool_call(obs):
            # Save current segment (excluding this handoff call)
            if current_segment:
                segments.append(current_segment[:])
            current_segment = []
            skip_until_new_agent = True
            continue
        
        # Check if this is a handoff/endCall result or related observation
        if is_handoff_result(obs):
            skip_until_new_agent = True
            continue
        
        # If we hit a new chat-completion after a handoff, start new segment
        if skip_until_new_agent and obs.get('name', '').startswith('chat-completion'):
            skip_until_new_agent = False
        
        # Add observation to current segment if we're not skipping
        if not skip_until_new_agent:
            current_segment.append(obs)
    
    # Add any remaining observations as a final segment
    if current_segment:
        segments.append(current_segment)
    
    return segments


def infer_agent_name(observations: List[Dict[str, Any]]) -> str:
    """Infer agent name from the observations in the segment"""
    # Look for handoff tool calls to determine the agent
    for obs in observations:
        output = obs.get('output')
        if isinstance(output, str):
            try:
                output = json.loads(output)
            except:
                continue
        
        if isinstance(output, dict) and output.get('type') == 'tool_calls':
            tool_calls = output.get('output', [])
            for tc in tool_calls:
                func_name = tc.get('function', {}).get('name', '')
                if 'handoff' in func_name.lower():
                    # Extract agent name from function name
                    # e.g., "handoff_to_dmv_appointment_creator" -> "DMV Appointment Creator"
                    name = func_name.replace('handoff_to_', '').replace('_', ' ').title()
                    return name
    
    # Fallback: use observation names or generic
    if observations:
        return observations[0].get('name', 'Agent').split('(')[0].strip()
    
    return "Agent"


def send_trace_to_langfuse(
    events: List[Dict[str, Any]],
    public_key: str,
    secret_key: str,
    host: str
) -> bool:
    """Send a batch of events to Langfuse"""
    api_url = f"{host}/api/public/ingestion"
    headers = {"Content-Type": "application/json"}
    auth = (public_key, secret_key)
    
    payload = {
        "batch": events,
        "metadata": {
            "batch_size": len(events),
            "sdk_integration": "trace_importer",
            "sdk_name": "python",
            "sdk_version": "custom"
        }
    }
    
    response = requests.post(
        api_url,
        json=payload,
        headers=headers,
        auth=auth,
        timeout=30
    )
    
    if response.status_code not in [200, 201, 207]:
        print(f"✗ Error sending data to Langfuse: {response.status_code}")
        print(f"Response: {response.text[:500]}")
        return False
    
    return True


def create_trace_from_observations(
    observations: List[Dict[str, Any]],
    trace_id: str,
    trace_name: str,
    public_key: str,
    secret_key: str,
    host: str,
    generate_new_ids: bool = True,
    agent_name: Optional[str] = None
) -> bool:
    """Create a single trace from a list of observations"""
    if not observations:
        return False
    
    sorted_obs = sorted(observations, key=lambda x: x.get('depth', 0))
    id_mapping = {}
    
    # Compute start and end time
    all_start_times = [obs.get("startTime") for obs in sorted_obs if obs.get("startTime")]
    all_end_times = [obs.get("endTime") for obs in sorted_obs if obs.get("endTime")]
    
    trace_start = min(all_start_times) if all_start_times else datetime.utcnow().isoformat() + "Z"
    trace_end = max(all_end_times) if all_end_times else trace_start
    
    # Merge all inputs/outputs
    merged_input, merged_output = collect_trace_io(sorted_obs)
    
    # Create metadata with agent name if provided
    metadata = {}
    if agent_name:
        metadata["agent_name"] = agent_name
    
    # Create trace event
    trace_event = {
        "id": str(uuid.uuid4()),
        "timestamp": trace_start,
        "type": "trace-create",
        "body": {
            "id": trace_id,
            "name": trace_name,
            "metadata": metadata,
            "startTime": trace_start,
            "endTime": trace_end,
            "input": merged_input,
            "output": merged_output
        }
    }
    
    events = [trace_event]
    
    # Create observation events
    for obs in sorted_obs:
        obs_id = obs.get('id')
        obs_type = obs.get('type', 'SPAN')
        
        if generate_new_ids:
            new_id = str(uuid.uuid4())
            id_mapping[obs_id] = new_id
            obs_id = new_id
        
        parent_id = obs.get('parentObservationId')
        if parent_id and generate_new_ids:
            parent_id = id_mapping.get(parent_id)
        
        if obs_type.upper() == 'GENERATION':
            event_type = "generation-create"
        elif obs_type.upper() == 'SPAN':
            event_type = "span-create"
        elif obs_type.upper() == 'EVENT':
            event_type = "event-create"
        else:
            event_type = "span-create"
        
        raw_metadata = normalize_tool_call_keys(obs.get("metadata", {}))
        metadata = raw_metadata if isinstance(raw_metadata, dict) else {}
        
        body = {
            "id": obs_id,
            "traceId": trace_id,
            "name": obs.get('name', f'{obs_type.lower()}-{obs_id[:8]}'),
            "startTime": obs.get('startTime'),
            "metadata": metadata,
        }
        
        if obs.get('endTime'):
            body["endTime"] = obs.get('endTime')
        
        if obs.get('input') is not None:
            body["input"] = normalize_tool_call_keys(obs.get('input'))
        
        if obs.get('output') is not None:
            body["output"] = normalize_tool_call_keys(obs.get('output'))
        
        if parent_id:
            body["parentObservationId"] = parent_id
        
        if obs_type.upper() == 'GENERATION':
            if obs.get('model'):
                body["model"] = obs.get('model')
            if obs.get('modelParameters'):
                body["modelParameters"] = obs.get('modelParameters')
            if obs.get('usage'):
                body["usage"] = obs.get('usage')
        
        if obs.get('level'):
            body["level"] = obs.get('level')
        if obs.get('statusMessage'):
            body["statusMessage"] = obs.get('statusMessage')
        if obs.get('version'):
            body["version"] = obs.get('version')
        
        event = {
            "id": str(uuid.uuid4()),
            "timestamp": obs.get('startTime') or datetime.utcnow().isoformat() + "Z",
            "type": event_type,
            "body": body
        }
        
        events.append(event)
    
    return send_trace_to_langfuse(events, public_key, secret_key, host)


def import_trace_to_langfuse(
    observations: List[Dict[str, Any]],
    public_key: str,
    secret_key: str,
    host: str,
    generate_new_ids: bool = True,
    create_agent_traces: bool = True
) -> str:
    if not observations:
        print("Error: No observations found in file")
        sys.exit(1)
    
    sorted_obs = sorted(observations, key=lambda x: x.get('depth', 0))
    root_obs = sorted_obs[0]
    trace_id = root_obs.get('traceId') or root_obs.get('id')
    
    if generate_new_ids:
        trace_id = str(uuid.uuid4())
    
    print(f"\n{'='*60}")
    print(f"Creating main trace: {trace_id}")
    print(f"{'='*60}")
    
    # Create main trace with all observations
    success = create_trace_from_observations(
        observations,
        trace_id,
        "Imported Trace (Full Conversation)",
        public_key,
        secret_key,
        host,
        generate_new_ids
    )
    
    if success:
        print(f"✓ Successfully imported main trace!")
        print(f"View at: {host}/trace/{trace_id}")
    else:
        print("✗ Failed to import main trace")
        return trace_id
    
    # Create agent-specific traces
    if create_agent_traces:
        print(f"\n{'='*60}")
        print("Creating agent-specific traces...")
        print(f"{'='*60}")
        
        agent_segments = extract_agent_segments(observations)
        
        for i, segment in enumerate(agent_segments, 1):
            agent_name = infer_agent_name(segment)
            agent_trace_id = str(uuid.uuid4()) if generate_new_ids else f"{trace_id}-agent-{i}"
            
            print(f"\n[Agent {i}/{len(agent_segments)}] {agent_name}")
            print(f"  Observations: {len(segment)}")
            print(f"  Trace ID: {agent_trace_id}")
            
            success = create_trace_from_observations(
                segment,
                agent_trace_id,
                f"{agent_name} Session",
                public_key,
                secret_key,
                host,
                generate_new_ids,
                agent_name=agent_name  # Pass agent name for metadata
            )
            
            if success:
                print(f"  ✓ Created agent trace")
                print(f"  View at: {host}/trace/{agent_trace_id}")
            else:
                print(f"  ✗ Failed to create agent trace")
    
    return trace_id


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('trace_file')
    parser.add_argument('--new-trace-id', action='store_true', default=True, help="Generate a new trace ID (default: True)")
    parser.add_argument('--no-agent-traces', action='store_true', help="Skip creating separate agent traces")
    parser.add_argument('--public-key')
    parser.add_argument('--secret-key')
    parser.add_argument('--host', default='https://us.cloud.langfuse.com')
    
    args = parser.parse_args()
    
    public_key = args.public_key or os.environ.get('LANGFUSE_PUBLIC_KEY')
    secret_key = args.secret_key or os.environ.get('LANGFUSE_SECRET_KEY')
    
    if not public_key or not secret_key:
        print("Error: Langfuse credentials not provided (check .env or CLI args)")
        sys.exit(1)
    
    print(f"Loading trace file: {args.trace_file}")
    observations = load_trace_file(args.trace_file)
    print(f"Found {len(observations)} observations")
    
    trace_id = import_trace_to_langfuse(
        observations,
        public_key,
        secret_key,
        args.host,
        generate_new_ids=args.new_trace_id,
        create_agent_traces=not args.no_agent_traces
    )
    
    print(f"\n{'='*60}")
    print("✓ Import complete!")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()