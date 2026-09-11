// Install the fixture before the production worker loads the native addon.
import "./delayed-native-load.cjs";
import "../../dist/native-worker.cjs";
