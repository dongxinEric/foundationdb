/*
 * StorageMetricsClientAPI.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct StorageMetricsClientAPIWorkload : TestWorkload {
	int64_t totalSize, nodeCount;

	double testDuration, transactionsPerSecond;
	vector<Future<Void>> clients;
	bool succeeded;

	StorageMetricsClientAPIWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), totalSize(0), succeeded(false) {
		testDuration = getOption(options, LiteralStringRef("testDuration"), 600.0);
		transactionsPerSecond = getOption(options, LiteralStringRef("transactionsPerSecond"), 5000.0) / clientCount;
		nodeCount = getOption(options, LiteralStringRef("nodeCount"), 1000);
	}

	virtual std::string description() { return "StorageMetricsClientAPI"; }

	virtual Future<Void> setup(Database const& cx) {

		if (clientId != 0) return Void();
		return _setup(cx, this);
	}

	virtual Future<Void> start(Database const& cx) {
		clients.push_back(
		    timeout(queryStorageMetrics(cx->clone(), this, 1 / transactionsPerSecond), testDuration, Void()));

		return delay(testDuration);
	}

	virtual Future<bool> check(Database const& cx) {
		if (clientId != 0) return true;
		return succeeded;
	}

	virtual void getMetrics(vector<PerfMetric>& m) {}

	ACTOR Future<Void> _setup(Database cx, StorageMetricsClientAPIWorkload* self) {
		state int g = 0;
		for (; g < 100; g++) {
			state ReadYourWritesTransaction tr(cx);
			state StringRef key;
			state Value value;
			loop {
				try {
					for (int i = 0; i < self->nodeCount / 100; i++) {
						key = StringRef(format("ops%08x%08x", g, i));
						value = self->randomString(value.arena(), deterministicRandom()->randomInt(10000, 20000));
						tr.set(key, value);
					}
					wait(tr.commit());
					self->totalSize += key.expectedSize() + value.expectedSize();
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}
		return Void();
	}

	ACTOR Future<Void> queryStorageMetrics(Database cx, StorageMetricsClientAPIWorkload* self, double delay) {
		state double lastTime = now();
		state int64_t estimatedSize = -1;
		loop {
			wait(poisson(&lastTime, delay));
			state ReadYourWritesTransaction tr(cx);
			loop {
				try {
					int64_t _estimatedSize = wait(tr.getEstimatedRangeSizeBytes(allKeys));
					estimatedSize = _estimatedSize;
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}

			if (estimatedSize <= 0 && estimatedSize > self->totalSize) {
				TraceEvent(SevError, "StorageMetricsClientAPIFailed")
				    .detail("TotalBytesWritten", self->totalSize)
				    .detail("EstimatedSizeReturned", estimatedSize);
				self->succeeded = false;
			} else {
				TraceEvent("StorageMetricsClientAPIPassed")
				    .detail("TotalBytesWritten", self->totalSize)
				    .detail("EstimatedSizeReturned", estimatedSize);
				self->succeeded = true;
			}
		}
	}

	StringRef randomString(Arena& arena, int len, char firstChar = 'a', char lastChar = 'z') {
		++lastChar;
		StringRef s = makeString(len, arena);
		for (int i = 0; i < len; ++i) {
			*(uint8_t*)(s.begin() + i) = (uint8_t)deterministicRandom()->randomInt(firstChar, lastChar);
		}
		return s;
	}
};

WorkloadFactory<StorageMetricsClientAPIWorkload> StorageMetricsClientAPIWorkloadFactory("StorageMetricsClientAPI");
