import boto3
import pandas as pd
import json
from datetime import datetime, timedelta

logs_client = boto3.client('logs', region_name='ap-northeast-2')
lambda_client = boto3.client('lambda', region_name='ap-northeast-2')

lambda_functions = ['slack_invitor', 'slack_invitor_invite_all', 'slack_invitor_convention']

for function_name in lambda_functions:
    print(f"{function_name} 함수의 로그 데이터를 추출합니다...")
    
    log_group_name = f"/aws/lambda/{function_name}"
    
    try:
        log_streams = []
        next_token = None
        
        while True:
            if next_token:
                response = logs_client.describe_log_streams(
                    logGroupName=log_group_name,
                    orderBy='LastEventTime',
                    descending=True,
                    nextToken=next_token
                )
            else:
                response = logs_client.describe_log_streams(
                    logGroupName=log_group_name,
                    orderBy='LastEventTime',
                    descending=True
                )
            
            log_streams.extend(response['logStreams'])
            
            if 'nextToken' in response:
                next_token = response['nextToken']
            else:
                break
        
        all_events = []

        for stream in log_streams:
            stream_name = stream['logStreamName']
            next_token = None
            
            while True:
                try:
                    if next_token:
                        response = logs_client.get_log_events(
                            logGroupName=log_group_name,
                            logStreamName=stream_name,
                            nextToken=next_token
                        )
                    else:
                        response = logs_client.get_log_events(
                            logGroupName=log_group_name,
                            logStreamName=stream_name
                        )
                    
                    events = response['events']
                    
                    for event in events:
                        message = event['message']
                        timestamp = datetime.fromtimestamp(event['timestamp'] / 1000)
                        
                        if "REPORT RequestId:" in message:
                            try:
                                parts = message.split('\t')
                                request_id = parts[0].split('RequestId: ')[1].strip()
                                
                                event_data = {
                                    'RequestId': request_id,
                                    'Timestamp': timestamp,
                                    'Function': function_name
                                }
                                
                                for part in parts[1:]:
                                    if part.strip():
                                        try:
                                            key, value = part.split(':', 1)
                                            event_data[key.strip()] = value.strip()
                                        except:
                                            pass
                                
                                all_events.append(event_data)
                            except Exception as e:
                                print(f"로그 파싱 오류: {e}")
                                print(f"문제의 메시지: {message}")
                    
                    if next_token == response['nextForwardToken']:
                        break
                    
                    next_token = response['nextForwardToken']
                    
                except Exception as e:
                    print(f"로그 이벤트 가져오기 오류: {e}")
                    break
        
        if all_events:
            df = pd.DataFrame(all_events)
            
            df = df.sort_values(by='Timestamp')
            
            for col in df.columns:
                if col not in ['RequestId', 'Timestamp', 'Function']:
                    try:
                        df[col] = df[col].str.extract(r'([\d\.]+)').astype(float)
                    except:
                        pass
            
            df.to_csv(f'{function_name}_events.csv', index=False)
            print(f"{len(df)} 개의 이벤트가 {function_name}_events.csv 파일로 저장되었습니다.")
        else:
            print(f"{function_name}에 대한 이벤트 데이터가 없습니다.")
    
    except Exception as e:
        print(f"{function_name} 함수 처리 중 오류 발생: {e}")
