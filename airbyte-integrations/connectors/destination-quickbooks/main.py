#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from destination_quickbooks import DestinationQuickbooks

if __name__ == "__main__":
    DestinationQuickbooks().run(sys.argv[1:])
