import boto3
import pandas as pd
import re
from datetime import datetime, timedelta

# AWS 클라이언트 생성
logs_client = boto3.client('logs', region_name='ap-northeast-2')

# 메트릭을 추출할 Lambda 함수 이름 목록
lambda_functions = ['slack_invitor_convention', 'slack_invitor_invite_all', 'slack_invitor']

for function_name in lambda_functions:
    print(f"{function_name} 함수의 로그 데이터를 추출합니다...")
    
    # 로그 그룹 이름
    log_group_name = f"/aws/lambda/{function_name}"
    
    # 모든 로그 이벤트를 저장할 리스트
    all_events = []
    
    try:
        # CloudWatch Logs Insights 쿼리 실행
        start_query_response = logs_client.start_query(
            logGroupName=log_group_name,
            startTime=int((datetime.now() - timedelta(days=365*2)).timestamp()),
            endTime=int(datetime.now().timestamp()),
            queryString="""
            filter @type = "REPORT"
            | parse @message /Duration: (?<Duration>.*) ms Billed Duration: (?<BilledDuration>.*) ms Memory Size: (?<MemorySize>.*) MB Max Memory Used: (?<MaxMemoryUsed>.*) MB/ 
            | parse @message /Init Duration: (?<InitDuration>.*) ms/ or parse @message /Init Duration: (?<InitDuration>.*) ms/
            | display @timestamp, @requestId as RequestId, Duration, BilledDuration, MemorySize, MaxMemoryUsed, InitDuration
            | sort @timestamp asc
            | limit 10000
            """
        )
        
        query_id = start_query_response['queryId']
        
        # 쿼리 완료 대기
        response = None
        while response is None or response['status'] == 'Running':
            print("쿼리 실행 중...")
            response = logs_client.get_query_results(
                queryId=query_id
            )
            
        # 결과 처리
        for result in response['results']:
            event_data = {'FunctionName': function_name}
            for field in result:
                field_name = field['field']
                field_value = field['value']
                
                if field_name == '@timestamp':
                    event_data['Timestamp'] = datetime.strptime(field_value, '%Y-%m-%d %H:%M:%S.%f')
                else:
                    try:
                        # 숫자로 변환 시도
                        event_data[field_name] = float(field_value) if field_value else None
                    except:
                        event_data[field_name] = field_value
            
            all_events.append(event_data)
        
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
