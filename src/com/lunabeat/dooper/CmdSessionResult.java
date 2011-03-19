/*
    Copyright [2011] [carlosdotdanger]

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */

package com.lunabeat.dooper;

/**
 *
 *
 */
public class CmdSessionResult {
	private int _code;
	private String _stderr;
	private String _stdout;

	public CmdSessionResult(int code,String stdout,String stderr){
		_code = code;
		_stdout = stdout;
		_stderr = stderr;
	}

	/**
	 * @return result code
	 */ public int getCode() {
		return _code;
	}

	/**
	 * @return stderr
	 */ public String getStderr() {
		return _stderr;
	}

	/**
	 * @return stdout
	 */ public String getStdout() {
		return _stdout;
	}
}
