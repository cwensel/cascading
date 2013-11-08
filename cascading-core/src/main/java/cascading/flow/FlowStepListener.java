package cascading.flow;

public interface FlowStepListener  {

   /**
   * The onStarting event is fired when a  given {@link cascading.flow.FlowStep} job has been submitted i.e. to hadoop cluster
   *
   * @param flowStep
   */
   public void onStepStarting(FlowStep<?> flowStep);
   
   /**
   * The onStepStopping event is fired when a given {@link cascading.flow.FlowStep} job is stopped
   *
   * @param flowStep 
   */
   public void onStepStopping(FlowStep<?> flowStep);

   /**
   * The onStepCompleted event is fired when a flowStepJob completed its work
   *
   * @param flowStep 
   */
   public void onStepCompleted(FlowStep<?> flowStep);
   
   /**
   * The onStepThrowable event is fired if a given {@link cascading.flow.FlowStep} throws a Throwable type. This throwable is passed
   * as an argument to the event. This event method should return true if the given throwable was handled and should
   * not be rethrown from the {@link Flow#complete()} method.
   *
   * @param flowStep
   * @param throwable
   * @return returns true if this listener has handled the given throwable
   */
   public boolean onStepThrowable(FlowStep<?> flowStep, Throwable throwable);

   /**
    * The onStepProgressing event is fired when a give {@link cascading.flow.FlowStep} makes a progress in
    * its task
    * @param flowStep
   */
   public void onStepProgressing(FlowStep<?> flowStep);
}
