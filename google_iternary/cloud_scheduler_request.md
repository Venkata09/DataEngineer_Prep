gcloud scheduler jobs create pubsub nms-daily-reporting \
--schedule="15 1 * * *" \
--time-zone="America/New_York" \
--topic="nms-daily-reporting" \
--message-body='{"mode":"all","excludeSchemas":["tmp","stage"]}'
