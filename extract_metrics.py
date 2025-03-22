import boto3
import pandas as pd
from datetime import datetime, timedelta

# CloudWatch 클라이언트 생성
cloudwatch = boto3.client('cloudwatch', region_name='ap-northeast-2')

# 메트릭을 추출할 Lambda 함수 이름 목록
lambda_functions = ['slack_invitor', 'slack_invitor_convention', 'slack_invitor_invite_all']

# 시간 범위 설정 (최근 7일)
end_time = datetime.now()
start_time = end_time - timedelta(days=7)

# 추출할 메트릭 목록
metrics = [
    'Duration',           # 함수 실행 시간
    'Invocations',        # 호출 횟수
    'Errors',             # 오류 수
    'Throttles',          # 제한 수
    'ConcurrentExecutions', # 동시 실행 수
    'IteratorAge'         # 이벤트 소스 매핑의 레코드 에이지
]

# 각 Lambda 함수에 대해 메트릭 추출
for function_name in lambda_functions:
    all_data = {}
    
    for metric in metrics:
        response = cloudwatch.get_metric_statistics(
            Namespace='AWS/Lambda',
            MetricName=metric,
            Dimensions=[{'Name': 'FunctionName', 'Value': function_name}],
            StartTime=start_time,
            EndTime=end_time,
            Period=3600,  # 1시간 간격
            Statistics=['Average', 'Maximum', 'Minimum', 'Sum']
        )
        
        # 데이터포인트 추출
        datapoints = response['Datapoints']
        
        # 데이터포인트가 있는 경우 데이터프레임으로 변환
        if datapoints:
            df = pd.DataFrame(datapoints)
            all_data[metric] = df
    
    # 모든 메트릭 데이터를 하나의 CSV 파일로 통합
    if all_data:
        # 각 메트릭의 데이터프레임을 하나로 합치기
        combined_data = pd.DataFrame()
        for metric, df in all_data.items():
            df['Metric'] = metric
            combined_data = pd.concat([combined_data, df])
        
        # CSV 파일로 저장
        combined_data.to_csv(f'{function_name}_metrics.csv', index=False)
        print(f"메트릭이 {function_name}_metrics.csv 파일로 저장되었습니다.")
    else:
        print(f"{function_name}에 대한 메트릭 데이터가 없습니다.")
