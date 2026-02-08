import typer
from dotenv import load_dotenv
from loguru import logger

from sqlexplore.library import log

load_dotenv()

app = typer.Typer(pretty_exceptions_enable=False, pretty_exceptions_show_locals=True)


@app.command()
def main(required_arg: str, optional_arg: str | None = None) -> int:
    log.configure()
    logger.info(f"Hello from sqlexplore! {required_arg=}, {optional_arg=}")
    logger.info("Finished.")
    return 0


if __name__ == "__main__":
    app()
