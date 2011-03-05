/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.lunabeat.pooper.commands;

/**
 *
 * @author cory
 */
class ArgumentException extends Exception {
	private String _problem;

	/**
	 * 
	 * @param problem
	 */
	ArgumentException(String problem){
		_problem = problem;
	}

	public String getProblem(){
		return _problem;
	}
}
