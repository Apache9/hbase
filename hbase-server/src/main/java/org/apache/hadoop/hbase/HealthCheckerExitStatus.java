package org.apache.hadoop.hbase;

public enum HealthCheckerExitStatus {
	SUCCESS,
	TIMED_OUT,
	FAILED_WITH_EXIT_CODE,
	FAILED_WITH_EXCEPTION,
	FAILED
}
