package org.jboss.resource.adapter.jms.inflow;

import javax.jms.ConnectionConsumer;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.ServerSessionPool;
import javax.jms.Session;

public class DelegatedConnectionConsumer implements ConnectionConsumer {
	JmsServerSessionPool pool;
	Thread thread;
	
	private class CheckForMessagesJob implements Runnable {
		JmsServerSessionPool pool;
		Destination destination;
		boolean doRun = true;
		
	    public void run() {
	    	JmsServerSession serverSession = null;
	    	while (doRun) {
	    		try {
	    			try {
	    				
	    			serverSession = (JmsServerSession)pool.getServerSession();
	    			if (serverSession != null) {
	    				Session session = serverSession.getSession();
	    				
	    				MessageConsumer consumer = session.createConsumer(destination);
	    				Message message = consumer.receiveNoWait();
	    				if (message != null) {
	    					serverSession.onMessage(message);
	    				}
	    				consumer.close();
	    			}
	    			}
	    			finally {
	    				
	    			}
	    		}
	    		catch (Exception ex) {
	    			ex.printStackTrace();
	    		}
	    		finally {
	    			pool.returnServerSession(serverSession);
	    		}
	    	}
	    }

	    public CheckForMessagesJob(JmsServerSessionPool pool, Destination destination) {
	      this.pool = pool;
	      this.destination = destination;
	    }
	    
	    public void terminate() {
	    	this.doRun = false;
	    }
	    
	  }
	
	CheckForMessagesJob job;
	
	public DelegatedConnectionConsumer(JmsServerSessionPool pool, Destination destination) {
		this.pool = pool;
		this.job = new CheckForMessagesJob(pool, destination);
		this.thread = new Thread(job);
		thread.start();
	}
	
	@Override
	public void close() throws JMSException {
		if (thread != null) {
			job.terminate();
            try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public ServerSessionPool getServerSessionPool() throws JMSException {
		return pool;
	}

}
