package org.openspaces.collections.exception;


public class GigaBlockingQueueCapcityReachedException extends RuntimeException  {
	
	private static final long serialVersionUID = 1L;

	public GigaBlockingQueueCapcityReachedException(String msg){
		super(msg);
	}
}
