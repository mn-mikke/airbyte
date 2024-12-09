#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from destination_h2ogpte import DestinationH2ogpte

if __name__ == "__main__":
    DestinationH2OGPTE().run(sys.argv[1:])
