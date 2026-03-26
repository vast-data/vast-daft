import marimo  # pyright: ignore[reportMissingImports]

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        """
        # S3 Explorer Notebook

        Explore AWS S3 buckets using Marimo's file browser.
        """
    )
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    from cloudpathlib import S3Client, S3Path

    return S3Client, S3Path


@app.cell
def _(mo, S3Client, S3Path):
    ENDPOINT = "http://vippool.ie-dev-pipeline.svc.cluster.local"
    BUCKET = "collections-bucket"
    ACCESS_KEY = "7P2486YDRB97497707R2"
    SECRET_KEY = "JGAD1JyssLJ3KQ1G2MQp06m/BsefdeZequVb008u"

    s3_client = S3Client(
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )
    s3_path = S3Path(f"s3://{BUCKET}/", client=s3_client)
    file_browser = mo.ui.file_browser(initial_path=s3_path, multiple=True)
    file_browser
    return ACCESS_KEY, BUCKET, ENDPOINT, SECRET_KEY, file_browser, s3_client, s3_path


@app.cell
def _(file_browser, mo):
    selected_files = [str(f.path) for f in file_browser.value] if file_browser.value else []
    mo.md(f"**Selected:** {selected_files}" if selected_files else "No files selected.")
    return (selected_files,)


if __name__ == "__main__":
    app.run()
