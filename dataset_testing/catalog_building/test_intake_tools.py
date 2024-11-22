import intake
import intake_tools


def test_intake_tools():
    cat = intake.open_catalog("sample_dataset.yaml")
    entry = cat.ngc4008
    for x in intake_tools.iterate_user_parameters(entry):
        print(x)


if __name__ == "__main__":
    test_intake_tools()
