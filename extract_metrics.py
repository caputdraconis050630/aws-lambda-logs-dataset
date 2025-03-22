import boto3
import pandas as pd
from datetime import datetime, timedelta

# CloudWatch 클라이언트 생성
cloudwatch = boto3.client('cloudwatch', region_name='ap-northeast-2')

# 메트릭을 추출할 Lambda 함수 이름 목록
lambda_functions = ['slack_invitor_convention', 'slack_invitor_invite_all', 'slack_invitor']

# 추출할 메트릭 목록
metrics = [
    'Duration',
    'Invocations',
    'Errors'
]

# 각 Lambda 함수에 대해 메트릭 추출
for function_name in lambda_functions:
    all_data = []
    
    for metric in metrics:
        paginator = cloudwatch.get_paginator('get_metric_data')
        
        end_time = datetime.now()
        start_time = end_time - timedelta(days=455)  # Max duration
        
        page_iterator = paginator.paginate(
            MetricDataQueries=[
                {
                    'Id': 'm1',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'AWS/Lambda',
                            'MetricName': metric,
                            'Dimensions': [
                                {
                                    'Name': 'FunctionName',
                                    'Value': function_name
                                },
                            ]
                        },
                        'Period': 300,
                        'Stat': 'Sum' if metric in ['Invocations', 'Errors'] else 'Average'
                    },
                    'ReturnData': True,
                },
            ],
            StartTime=start_time,
            EndTime=end_time,
        )
        
        for page in page_iterator:
            timestamps = page['MetricDataResults'][0]['Timestamps']
            values = page['MetricDataResults'][0]['Values']
            
            for timestamp, value in zip(timestamps, values):
                all_data.append({
                    'Timestamp': timestamp,
                    'Metric': metric,
                    'Value': value
                })
    
    # 데이터프레임으로 변환
    if all_data:
        df = pd.DataFrame(all_data)
        
        # Timestamp 기준으로 오름차순 정렬
        df = df.sort_values(by='Timestamp')
        
        # CSV 파일로 저장
        df.to_csv(f'{function_name}_metrics.csv', index=False)
        print(f"메트릭이 {function_name}_metrics.csv 파일로 저장되었습니다.")
    else:
        print(f"{function_name}에 대한 메트릭 데이터가 없습니다.")
