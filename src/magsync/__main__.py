"""Entry point for magsync."""


def main():
    """Launch magsync - TUI by default, CLI when subcommands are provided."""
    from magsync.cli import app

    app()


if __name__ == "__main__":
    main()
