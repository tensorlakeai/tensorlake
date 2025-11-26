import traceback


def print_exception(e: BaseException) -> None:
    # In remote mode we only print exceptions and we don't propagate them to
    # SDK remote clients. We return a generic RequestFailed instead from
    # remote clients. To do the same in local mode, we print the exception here.
    #
    # KeyboardInterrupt is intentional by user, no need to print it.
    if not isinstance(e, KeyboardInterrupt):
        print("\n")
        traceback.print_exception(e)
