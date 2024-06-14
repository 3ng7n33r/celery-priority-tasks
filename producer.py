import logging
from worker_files.models import ModelwithMethod

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    for i in range(3):
        obj = ModelwithMethod(i)
        obj.load()


if __name__ == "__main__":
    main()
