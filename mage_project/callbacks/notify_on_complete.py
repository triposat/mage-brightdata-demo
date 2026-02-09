"""
Callback: Notify on Pipeline Completion

Sends a summary notification when the pipeline completes.
Can be extended to send Slack, email, or webhook notifications.
"""

if 'callback' not in dir():
    from mage_ai.data_preparation.decorators import callback


@callback('on_success')
def on_success(block_run, *args, **kwargs):
    """
    Called when the pipeline runs successfully.

    Prints a summary and can be extended to send notifications.
    """
    print("=" * 60)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 60)

    # Get block output info if available
    if hasattr(block_run, 'output'):
        output = block_run.output
        if output is not None and hasattr(output, '__len__'):
            print(f"Products processed: {len(output)}")

    print("")
    print("Next steps:")
    print("  - Check PostgreSQL for stored data")
    print("  - Review CSV exports in /output folder")
    print("  - View charts in Mage UI")
    print("=" * 60)

    # TODO: Add Slack notification
    # slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    # if slack_webhook_url:
    #     requests.post(slack_webhook_url, json={
    #         "text": f"Amazon scraping complete! {len(output)} products processed."
    #     })

    return {}


@callback('on_failure')
def on_failure(block_run, *args, **kwargs):
    """
    Called when the pipeline fails.
    """
    print("=" * 60)
    print("PIPELINE FAILED")
    print("=" * 60)

    if hasattr(block_run, 'error'):
        print(f"Error: {block_run.error}")

    # TODO: Add alert notification
    # Send urgent notification via Slack/email

    return {}
