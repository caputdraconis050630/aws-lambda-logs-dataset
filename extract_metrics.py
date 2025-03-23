import boto3
import json
import pandas as pd
from datetime import datetime
import os

# AWS 클라이언트 설정
client = boto3.client('logs')

# 로그를 추출할 Lambda 함수 목록
lambda_functions = ["slack_invitor", "slack_invitor_invite_all", "slack_invitor_convention"]

def extract_logs_for_function(function_name):
    """특정 Lambda 함수의 모든 로그를 추출하여 데이터프레임으로 반환합니다."""
    log_group_name = f"/aws/lambda/{function_name}"
    all_events = []
    
    # 로그 스트림 목록 가져오기
    paginator = client.get_paginator('describe_log_streams')
    for page in paginator.paginate(logGroupName=log_group_name):
        for stream in page['logStreams']:
            # 각 로그 스트림에서 이벤트 가져오기
            log_events_paginator = client.get_paginator('filter_log_events')
            for log_page in log_events_paginator.paginate(
                logGroupName=log_group_name,
                logStreamNames=[stream['logStreamName']]
            ):
                for event in log_page['events']:
                    try:
                        # 로그 메시지 파싱
                        log_data = {
                            'timestamp': datetime.fromtimestamp(event['timestamp'] / 1000).isoformat(),
                            'message': event['message'],
                            'requestId': extract_request_id(event['message']),
                            'duration': extract_duration(event['message']),
                            'billedDuration': extract_billed_duration(event['message']),
                            'memorySize': extract_memory_size(event['message']),
                            'maxMemoryUsed': extract_max_memory_used(event['message'])
                        }
                        all_events.append(log_data)
                    except Exception as e:
                        print(f"Error processing log event: {e}")
    
    # 데이터프레임 생성 및 타임스탬프로 정렬
    if all_events:
        df = pd.DataFrame(all_events)
        df = df.sort_values(by='timestamp')
        return df
    else:
        return pd.DataFrame()

def extract_request_id(message):
    """로그 메시지에서 RequestId 추출"""
    if "RequestId:" in message:
        try:
            return message.split("RequestId:")[1].split()[0]
        except:
            pass
    return None

def extract_duration(message):
    """로그 메시지에서 Duration 추출"""
    if "Duration:" in message and "ms" in message:
        try:
            return float(message.split("Duration:")[1].split("ms")[0].strip())
        except:
            pass
    return None

def extract_billed_duration(message):
    """로그 메시지에서 Billed Duration 추출"""
    if "Billed Duration:" in message and "ms" in message:
        try:
            return float(message.split("Billed Duration:")[1].split("ms")[0].strip())
        except:
            pass
    return None

def extract_memory_size(message):
    """로그 메시지에서 Memory Size 추출"""
    if "Memory Size:" in message and "MB" in message:
        try:
            return float(message.split("Memory Size:")[1].split("MB")[0].strip())
        except:
            pass
    return None

def extract_max_memory_used(message):
    """로그 메시지에서 Max Memory Used 추출"""
    if "Max Memory Used:" in message and "MB" in message:
        try:
            return float(message.split("Max Memory Used:")[1].split("MB")[0].strip())
        except:
            pass
    return None

def main():
    """모든 Lambda 함수의 로그를 추출하고 CSV 파일로 저장합니다."""
    for function_name in lambda_functions:
        print(f"Extracting logs for {function_name}...")
        df = extract_logs_for_function(function_name)
        
        if not df.empty:
            # CSV 파일로 저장
            output_file = f"{function_name}_logs.csv"
            df.to_csv(output_file, index=False)
            print(f"Saved {len(df)} log entries to {output_file}")
        else:
            print(f"No logs found for {function_name}")

if __name__ == "__main__":
    main()
