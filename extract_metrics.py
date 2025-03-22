import boto3
import pandas as pd
import json
from datetime import datetime, timedelta

# CloudWatch Logs 클라이언트 생성
logs_client = boto3.client('logs', region_name='ap-northeast-2')
lambda_client = boto3.client('lambda', region_name='ap-northeast-2')

# 메트릭을 추출할 Lambda 함수 이름 목록
lambda_functions = ['slack_invitor', 'slack_invitor_convention', 'slack_invitor_invite_all']

# 각 Lambda 함수에 대해 로그 데이터 추출
for function_name in lambda_functions:
    print(f"{function_name} 함수의 로그 데이터를 추출합니다...")
    
    # 로그 그룹 이름 (Lambda 함수의 로그 그룹은 /aws/lambda/함수이름 형식)
    log_group_name = f"/aws/lambda/{function_name}"
    
    try:
        # 로그 스트림 목록 가져오기 (최신 순으로 정렬)
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
        
        # 이벤트 데이터를 저장할 리스트
        all_events = []
        
        # 각 로그 스트림에서 이벤트 가져오기
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
                        
                        # REPORT 로그 라인에서 메트릭 추출
                        if "REPORT RequestId:" in message:
                            try:
                                parts = message.split('\t')
                                request_id = parts[0].split('RequestId: ')[1].strip()
                                
                                event_data = {
                                    'RequestId': request_id,
                                    'Timestamp': timestamp,
                                    'Function': function_name
                                }
                                
                                # 각 메트릭 추출
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
                    
                    # 다음 토큰이 이전과 같으면 반복 중단
                    if next_token == response['nextForwardToken']:
                        break
                    
                    next_token = response['nextForwardToken']
                    
                except Exception as e:
                    print(f"로그 이벤트 가져오기 오류: {e}")
                    break
        
        # 데이터프레임으로 변환
        if all_events:
            df = pd.DataFrame(all_events)
            
            # Timestamp 기준으로 오름차순 정렬
            df = df.sort_values(by='Timestamp')
            
            # 숫자 데이터 정리 (예: "2.78 ms" -> 2.78)
            for col in df.columns:
                if col not in ['RequestId', 'Timestamp', 'Function']:
                    try:
                        df[col] = df[col].str.extract(r'([\d\.]+)').astype(float)
                    except:
                        pass
            
            # CSV 파일로 저장
            df.to_csv(f'{function_name}_events.csv', index=False)
            print(f"{len(df)} 개의 이벤트가 {function_name}_events.csv 파일로 저장되었습니다.")
        else:
            print(f"{function_name}에 대한 이벤트 데이터가 없습니다.")
    
    except Exception as e:
        print(f"{function_name} 함수 처리 중 오류 발생: {e}")
