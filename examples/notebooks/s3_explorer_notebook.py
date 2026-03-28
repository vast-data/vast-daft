import marimo  # type: ignore

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md("""
    # S3 Explorer Notebook

    Explore AWS S3 buckets using Marimo's file browser.
    """)
    return


@app.cell
def _():
    import marimo as mo  # type: ignore

    return (mo,)


@app.cell
def _():
    from cloudpathlib import S3Client, S3Path  # type: ignore
    from helpers import get_s3_credentials  # type: ignore

    return S3Client, S3Path, get_s3_credentials


@app.cell
def _(S3Client, S3Path, get_s3_credentials, mo):
    ENDPOINT = "http://vippool.ie-dev-pipeline.svc.cluster.local"
    BUCKET = "collections-bucket"
    ACCESS_KEY, SECRET_KEY = get_s3_credentials()

    s3_client = S3Client(
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )
    s3_path = S3Path(f"s3://{BUCKET}/", client=s3_client)
    file_browser = mo.ui.file_browser(initial_path=s3_path, multiple=True)
    file_browser
    return (file_browser,)


@app.cell
def _(file_browser, mo):
    selected_files = [str(f.path) for f in file_browser.value] if file_browser.value else []
    mo.md(f"**Selected:** {selected_files}" if selected_files else "No files selected.")
    return


if __name__ == "__main__":
    app.run()
