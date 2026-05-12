def on_failure_callback(context: dict) -> None:
    from airflow.exceptions import AirflowNotFoundException
    from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

    msg = (
        f":red_circle: DAG *{context['dag'].dag_id}* failed.\n"
        f"Task: `{context['task_instance'].task_id}`\n"
        f"Run: `{context['run_id']}`\n"
        f"Log: {context['task_instance'].log_url}"
    )

    try:
        SlackWebhookOperator(
            task_id="slack_alert",
            slack_webhook_conn_id="slack_default",
            message=msg,
        ).execute(context)
    except AirflowNotFoundException:
        print("WARNING: slack_default connection not configured — skipping Slack alert")
