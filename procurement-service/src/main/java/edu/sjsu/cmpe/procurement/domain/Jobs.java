package edu.sjsu.cmpe.procurement.domain;

import javax.jms.JMSException;

import org.json.JSONException;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

public abstract class Jobs implements org.quartz.Job {

    private final Timer timer;

    public Jobs() {
        timer = Metrics.defaultRegistry().newTimer(getClass(), getClass().getName());
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        TimerContext timerContext = timer.time();
        try {
            doJob();
        } catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
            timerContext.stop();
        }
    }

    public abstract void doJob() throws JMSException, JSONException;
}
