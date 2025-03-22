import boto3
import pandas as pd
import re
from datetime import datetime, timedelta

# AWS 클라이언트 생성
logs_client = boto3.client('logs', region_name='ap-northeast-2')
lambda_client = boto3.client('lambda', region_name='ap-northeast-2')

# 메트릭을 추출할 Lambda 함수 이름 목록
lambda_functions = ['slack_invitor_convention', 'slack_invitor_invite_all', 'slack_invitor']

for function_name in lambda_functions:
    print(f"{function_name} 함수의 로그 데이터를 추출합니다...")
    
    # 로그 그룹 이름
    log_group_name = f"/aws/lambda/{function_name}"
    
    # 로그 스트림 목록 가져오기
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
        
        # 모든 로그 이벤트를 저장할 리스트
        all_events = []
        
        # 각 로그 스트림에서 REPORT 로그 이벤트 가져오기
        for stream in log_streams:
            stream_name = stream['logStreamName']
            next_token = None
            
            try:
                while True:
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
                                request_id = re.search(r'RequestId: ([0-9a-f-]+)', message).group(1)
                                
                                # 기본 정보 설정
                                event_data = {
                                    'Timestamp': timestamp,
                                    'RequestId': request_id,
                                    'FunctionName': function_name
                                }
                                
                                # Duration (실행 시간) 추출
                                duration_match = re.search(r'Duration: ([\d.]+) ms', message)
                                if duration_match:
                                    event_data['Duration'] = float(duration_match.group(1))
                                
                                # Billed Duration (청구된 시간) 추출
                                billed_duration_match = re.search(r'Billed Duration: ([\d.]+) ms', message)
                                if billed_duration_match:
                                    event_data['BilledDuration'] = float(billed_duration_match.group(1))
                                
                                # Memory Size (할당된 메모리) 추출
                                memory_size_match = re.search(r'Memory Size: ([\d.]+) MB', message)
                                if memory_size_match:
                                    event_data['MemorySize'] = float(memory_size_match.group(1))
                                
                                # Max Memory Used (사용된 최대 메모리) 추출
                                max_memory_match = re.search(r'Max Memory Used: ([\d.]+) MB', message)
                                if max_memory_match:
                                    event_data['MaxMemoryUsed'] = float(max_memory_match.group(1))
                                
                                # Init Duration (초기화 시간, 콜드 스타트) 추출
                                init_duration_match = re.search(r'Init Duration: ([\d.]+) ms', message)
                                if init_duration_match:
                                    event_data['InitDuration'] = float(init_duration_match.group(1))
                                
                                all_events.append(event_data)
                            except Exception as e:
                                print(f"로그 파싱 오류: {e}")
                    
                    # 다음 토큰이 이전과 같으면 반복 중단
                    if next_token == response['nextForwardToken']:
                        break
                    
                    next_token = response['nextForwardToken']
                    
            except Exception as e:
                print(f"로그 이벤트 가져오기 오류: {e}")
        
        # 데이터프레임으로 변환
        if all_events:
            df = pd.DataFrame(all_events)
            
            # Timestamp 기준으로 오름차순 정렬
            df = df.sort_values(by='Timestamp')
            
            # CSV 파일로 저장
            df.to_csv(f'{function_name}_metrics.csv', index=False)
            print(f"{len(df)}개의 이벤트가 {function_name}_metrics.csv 파일로 저장되었습니다.")
        else:
            print(f"{function_name}에 대한 이벤트 데이터가 없습니다.")
    
    except Exception as e:
        print(f"{function_name} 함수 처리 중 오류 발생: {e}")
