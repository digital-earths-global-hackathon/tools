import intake
import intake_tools
from pathlib import Path


def test_intake_tools():
    cat = intake.open_catalog(Path(__file__).parent/"sample_dataset.yaml")
    entry = cat.ngc4008
    for x in intake_tools.iterate_user_parameters(entry):
        print(x)


if __name__ == "__main__":
    test_intake_tools()
