"""Operator command for taking over an absent or expired site lease."""

import argparse
import asyncio

from botocore.exceptions import ClientError
from jammy_common.config import ConfigError

from .lease import Lease


def main() -> None:
    parser = argparse.ArgumentParser(prog="dispatcher")
    commands = parser.add_subparsers(required=True)
    lease_cmd = commands.add_parser("lease")
    lease_commands = lease_cmd.add_subparsers(required=True)
    takeover = lease_commands.add_parser("takeover")
    takeover.add_argument("--confirm", action="store_true", required=True)
    args = parser.parse_args()
    try:
        epoch = asyncio.run(Lease.from_env().takeover(args.confirm))
    except ClientError as error:
        code = error.response["Error"]["Code"]
        if code == "ConditionalCheckFailedException":
            parser.exit(1, "Lease takeover requires an absent or expired lease.\n")
        parser.exit(1, f"Lease takeover failed ({code}).\n")
    except (ConfigError, ValueError) as error:
        parser.exit(1, f"{error}\n")
    print(epoch)


if __name__ == "__main__":
    main()
