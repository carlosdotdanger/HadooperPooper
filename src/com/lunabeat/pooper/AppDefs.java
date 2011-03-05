/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lunabeat.pooper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author cory
 */
public final class AppDefs {

	public static final Map<String, AppCommand> COMMANDS = initCommands();

	//do not allow instantiation
	private AppDefs() {
	}

	private AppDefs(AppDefs a) {
	}

	private static Map<String, AppCommand> initCommands() {
		Map<String, AppCommand> tmpMap = new HashMap<String, AppCommand>();
		tmpMap.put("list-clusters", new AppCommand('l', "list-clusters", CommandType.Boolean,"List all ec2-haoop cluster names."));
		return Collections.unmodifiableMap(tmpMap);
	}

	/**
	 * Encapsulates a command-line arg, its description for help, and values it takes.
	 */
	public static final class AppCommand {

		private char _shortArg;
		private String _longArg;
		private String _description;
		private CommandType _type;

		/**
		 * 
		 * @param shortArg
		 * @param longArg
		 * @param description
		 */
		public AppCommand(char shortArg, String longArg, CommandType type, String description) {
			_shortArg = shortArg;
			_longArg = longArg;
			_description = description;
			_type = type;
		}

		/**
		 * @return the short arg
		 */
		public char shortArg() {
			return _shortArg;
		}

		/**
		 * @return the long arg
		 */
		public String longArg() {
			return _longArg;
		}

		/**
		 * @return the description
		 */
		public String description() {
			return _description;
		}

		/**
		 * @return the type
		 */
		public CommandType type() {
			return _type;
		}
	}

	public enum CommandType {
		Boolean,
		String,
		Integer,
		Float,
		Multiple
	}
}
