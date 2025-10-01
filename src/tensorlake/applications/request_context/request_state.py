from tensorlake.applications.user_data_serializer import PickleUserDataSerializer

# User data stored in request context is not available via HTTP.
# So we can use Pickle which gives smooth UX.
REQUEST_STATE_USER_DATA_SERIALIZER = PickleUserDataSerializer()
