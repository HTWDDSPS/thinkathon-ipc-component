import boto3

client = boto3.client('iotevents-data')


response = client.list_alarms(
    alarmModelName='Wertueberschreitung_assetModel_013041fc-0dcb-4540-b53c-4457a77bff5f'
)

print(response)