"""Clone bounded pinned inputs into a new worker-owned execution directory."""

import hashlib
import re
import shutil
from pathlib import Path


