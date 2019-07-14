import { complianceTestsAsync } from "@keeveestore/test-suite";
import { StoreAsync } from "../src/async";

complianceTestsAsync(() => StoreAsync.new<string, number>({ connection: "ckvs" }), {
	"1": 1,
	"2": 2,
	"3": 3,
	"4": 4,
	"5": 5,
});
