import logging

logger = logging.getLogger("intake_tools")


def iterate_user_parameters(entry, fixed_keys=None):
    if fixed_keys is None:
        fixed_keys = {}
    user_parameters = entry.describe()["user_parameters"]
    names = [x["name"] for x in user_parameters]
    remaining_keys = [x for x in names if x not in fixed_keys.keys()]
    if len(remaining_keys) == 0:
        return
    param = user_parameters[len(fixed_keys)]
    logger.debug(param)
    key = param["name"]
    for value in param["allowed"]:
        fixed_keys[key] = value
        logger.debug(fixed_keys)
        if len(remaining_keys) == 1:
            logger.debug(fixed_keys)
            yield fixed_keys
        else:
            yield from iterate_user_parameters(entry, fixed_keys.copy())
