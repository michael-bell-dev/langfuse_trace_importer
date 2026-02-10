#!/usr/bin/env python3
"""
Langfuse Trace Importer

This script reads an exported Langfuse trace JSON file and creates a new trace
in Langfuse with the same structure and data using the Public API.
"""

import json
import argparse
import sys
from datetime import datetime
from typing import List, Dict, Any
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


def parse_json_if_string(value: Any) -> Any:
    """Parse JSON strings into objects, preserving structure"""
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            # Recursively parse nested structures
            return parse_json_if_string(parsed)
        except (json.JSONDecodeError, ValueError):
            # Not valid JSON, return as-is
            return value
    elif isinstance(value, dict):
        return {k: parse_json_if_string(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [parse_json_if_string(v) for v in value]
    return value


def transform_tool_calls_output(output_data: Any) -> Any:
    """Transform tool_calls output format to Langfuse expected structure"""
    if not isinstance(output_data, dict):
        return output_data
    
    # Check if this is a tool_calls type output
    if output_data.get("type") == "tool_calls" and "output" in output_data:
        tool_calls_list = output_data["output"]
        if isinstance(tool_calls_list, list):
            # Transform to the structure with toolCalls array and content/contents
            transformed = {
                "toolCalls": []
            }
            
            for tool_call in tool_calls_list:
                if isinstance(tool_call, dict):
                    # Create the nested structure expected by Langfuse
                    transformed_call = {
                        "toolCall": {
                            "id": tool_call.get("id", ""),
                            "name": tool_call.get("function", {}).get("name", "") if "function" in tool_call else tool_call.get("name", ""),
                            "input": {}
                        }
                    }
                    
                    # Parse the arguments if present
                    if "function" in tool_call and "arguments" in tool_call["function"]:
                        args = tool_call["function"]["arguments"]
                        if isinstance(args, str):
                            try:
                                transformed_call["toolCall"]["input"] = json.loads(args)
                            except:
                                transformed_call["toolCall"]["input"] = {}
                        elif isinstance(args, dict):
                            transformed_call["toolCall"]["input"] = args
                    
                    transformed["toolCalls"].append(transformed_call)
            
            # Add content and contents fields
            transformed["content"] = " "
            transformed["contents"] = []
            
            return transformed
    
    return output_data


def normalize_tool_call_keys(value: Any) -> Any:
    """Recursively convert camelCase tool call keys to snake_case"""
    if isinstance(value, str):
        # For string values, replace the camelCase patterns
        return (
            value
            .replace("toolCallId", "tool_call_id")
            .replace("toolCalls", "tool_calls")
            .replace("toolCall", "tool_call")
        )
    elif isinstance(value, dict):
        result = {}
        for k, v in value.items():
            # Convert the key itself
            new_key = k
            if k == "toolCallId":
                new_key = "tool_call_id"
            elif k == "toolCalls":
                new_key = "tool_calls"
            elif k == "toolCall":
                new_key = "tool_call"
            
            # Recursively process the value
            result[new_key] = normalize_tool_call_keys(v)
        return result
    elif isinstance(value, list):
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
    """Get the input and output from the last chat-completion observation"""
    final_input = None
    final_output = None
    
    # Find the last chat-completion observation (skip tool-call and tool-start-message)
    for obs in reversed(observations):
        obs_name = obs.get('name', '')
        
        # Skip tool-call and tool-start-message observations
        if 'tool-call' in obs_name or 'tool-start-message' in obs_name:
            continue
            
        # Look for chat-completion observations
        if 'chat-completion' in obs_name:
            if obs.get('input') is not None:
                input_data = parse_json_if_string(obs['input'])
                final_input = normalize_tool_call_keys(input_data)
            
            if obs.get('output') is not None:
                output_data = parse_json_if_string(obs['output'])
                output_data = transform_tool_calls_output(output_data)
                final_output = normalize_tool_call_keys(output_data)
            
            break
    
    return final_input, final_output


def import_trace_to_langfuse(
    observations: List[Dict[str, Any]],
    public_key: str,
    secret_key: str,
    host: str,
    generate_new_ids: bool = True
) -> str:
    if not observations:
        print("Error: No observations found in file")
        sys.exit(1)
    
    sorted_obs = sorted(observations, key=lambda x: x.get('depth', 0))
    id_mapping = {}
    
    root_obs = sorted_obs[0]
    trace_id = root_obs.get('traceId') or root_obs.get('id')
    
    if generate_new_ids:
        new_trace_id = str(uuid.uuid4())
        id_mapping[trace_id] = new_trace_id
        trace_id = new_trace_id
    
    print(f"Creating trace: {trace_id}")
    
    api_url = f"{host}/api/public/ingestion"
    headers = {"Content-Type": "application/json"}
    auth = (public_key, secret_key)
    
    raw_trace_metadata = normalize_tool_call_keys(root_obs.get("metadata", {}))
    trace_metadata = raw_trace_metadata if isinstance(raw_trace_metadata, dict) else {}

    # Compute start and end time for trace overview
    all_start_times = [obs.get("startTime") for obs in sorted_obs if obs.get("startTime")]
    all_end_times = [obs.get("endTime") for obs in sorted_obs if obs.get("endTime")]

    trace_start = min(all_start_times) if all_start_times else datetime.utcnow().isoformat() + "Z"
    trace_end = max(all_end_times) if all_end_times else trace_start

    # Get input/output from last chat-completion
    merged_input, merged_output = collect_trace_io(sorted_obs)

    trace_event = {
        "id": str(uuid.uuid4()),
        "timestamp": trace_start,
        "type": "trace-create",
        "body": {
            "id": trace_id,
            "name": root_obs.get("name", "Imported Trace"),
            "metadata": trace_metadata,
            "startTime": trace_start,
            "endTime": trace_end,
            "input": merged_input,
            "output": merged_output
        }
    }

    events = [trace_event]
    
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
            # Parse JSON string if needed, then normalize
            input_data = parse_json_if_string(obs.get('input'))
            body["input"] = normalize_tool_call_keys(input_data)
        
        if obs.get('output') is not None:
            # Parse JSON string if needed, then normalize
            output_data = parse_json_if_string(obs.get('output'))
            # Transform tool_calls format if present
            output_data = transform_tool_calls_output(output_data)
            body["output"] = normalize_tool_call_keys(output_data)
        
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
        print(f"  Prepared {obs_type}: {obs.get('name')} ({obs_id[:8]}...)")
    
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
    
    print(f"\nAPI Response Status: {response.status_code}")
    print(f"API Response Body: {response.text[:500]}")
    
    if response.status_code not in [200, 201, 207]:
        print("✗ Error sending data to Langfuse")
        sys.exit(1)
    
    return trace_id


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('trace_file')
    parser.add_argument('--new-trace-id', action='store_true', default=True, help="Generate a new trace ID (default: True)")
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
        generate_new_ids=args.new_trace_id
    )
    
    print(f"\n✓ Successfully imported trace!")
    print(f"Trace ID: {trace_id}")
    print(f"View at: {args.host}/trace/{trace_id}")


if __name__ == '__main__':
    main()
