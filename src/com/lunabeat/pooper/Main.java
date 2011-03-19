/***********************************************
*    Copyright [2011] [carlosdotdanger]
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*/
package com.lunabeat.pooper;

import com.lunabeat.dooper.ClusterConfig;
import com.lunabeat.dooper.ClusterConfig.ConfigException;
import com.lunabeat.pooper.commands.AppCommand;
import java.io.IOException;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author cory
 */
public class Main {
	private static final String DEFAULT_CONFIG_PATH = "pooper.properties";
	public static void main(String[] args) {
		if(args.length < 1){
			System.out.println("USAGE: pooper <COMMAND> [command opts...]");
			System.out.println("available commands:");
			for(String c : AppCommand.commandNames()){
				System.out.println("\t"+ c);
			}
			System.exit(1);
		}
		if(System.getProperty("logging.config") != null){
				PropertyConfigurator.configure(System.getProperty("logging.config"));
		}

		String configPath = System.getProperty("pooper.config",DEFAULT_CONFIG_PATH);
		try{
			ClusterConfig conf = new ClusterConfig(configPath);
			AppCommand appcom = new AppCommand(conf);
			String command = args[0];
			String[] commandArgs = new String[args.length -1];
			for(int x = 1; x < args.length;x++){
				commandArgs[x-1] = args[x];
			}
			appcom.runAppCommand(command,commandArgs);
		}catch(IOException e){
			System.out.println("could not open config file '" + configPath + "'");
			System.exit(1);
		}catch(ConfigException c){
			System.out.println(c.getMessage());
			System.exit(1);
		}

	}


}
