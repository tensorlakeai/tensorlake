import traceback


def print_user_exception(e: BaseException) -> None:
    # We only print exceptions in remote mode and we don't propagate them to
    # SDK remote clients. We return a generic RequestFailureException instead
    # from remote clients. Do the same in local mode, we print the exception here.
    print("\nException during local request run:\n")
    traceback.print_exception(e)
