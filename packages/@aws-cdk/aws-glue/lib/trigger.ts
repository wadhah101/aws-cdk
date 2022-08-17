import * as events from '@aws-cdk/aws-events';
import * as iam from '@aws-cdk/aws-iam';
import * as cdk from '@aws-cdk/core';
import * as constructs from 'constructs';
import { CfnTrigger } from './glue.generated';
/**
 * The type of predefined worker that is allocated when a job runs.
 *
 * If you need to use a WorkerType that doesn't exist as a static member, you
 * can instantiate a `WorkerType` object, e.g: `WorkerType.of('other type')`.
 */
export enum TriggerType {
  /**
   * TODO
   */
  SCHEDULED = 'SCHEDULED',
  EVENT = 'EVENT',
  ON_DEMAND = 'ON_DEMAND',
  CONDITIONAL = 'CONDITIONAL'
}

export enum TriggerState {
  /**
   * TODO
   */
  CREATED = 'CREATED',

  /**
   * State indicating job run failed
   */
  ACTIVATED = ' ACTIVATED',

  /**
   * State indicating job run timed out
   */
  DEACTIVATED = 'DEACTIVATED',

  /**
   * State indicating job is starting
   */
  ACTIVATING = 'ACTIVATING',
  CREATING = 'CREATING',
  DEACTIVATING = 'DEACTIVATING',
  DELETING = 'DELETING',
  UPDATING = 'UPDATING'
}


/**
 * Interface representing a created or an imported {@link Trigger}.
 */
export interface ITrigger extends cdk.IResource {
  /**
   * The name of the trigger.
   * @attribute
   */
  readonly triggerName: string;

  /**
   * The ARN of the image.png.
   * @attribute
   */
  readonly triggerArn: string;

}

abstract class TriggerBase extends cdk.Resource implements ITrigger {
  public abstract readonly triggerArn: string;
  public abstract readonly triggerName: string;
  // public abstract readonly grantPrincipal: iam.IPrincipal;
}

/**
 * Attributes for importing {@link Job}.
 */
export interface TriggerAttributes {
  /**
   * The name of the job.
   */
  readonly triggerName: string;

  /**
   * The IAM role assumed by Glue to run this job.
   *
   * @default - undefined
   */
  readonly role?: iam.IRole;
}


export interface TriggerBaseProps {

  readonly actions: any[]

  readonly description?: string,

  readonly workflowName?: string

  readonly triggerName?: string;

  readonly tags?: { [key: string]: string };
}

/**
 * Construction properties for {@link Job}.
 */
export interface ScheduledTriggerProps extends TriggerBaseProps {

  readonly schedule: events.Schedule,

  readonly startOnCreation?: boolean
}
/**
 * A Glue Job.
 */
export class ScheduledTrigger extends TriggerBase {

  /**
   * The ARN of the job.
   */
  public readonly triggerArn: string;

  /**
   * The name of the job.
   */
  public readonly triggerName: string;

  /**
   * The IAM role Glue assumes to run this job.
   */
  // public readonly role: iam.IRole;

  /**
   * The principal this Glue Job is running as.
   */
  // public readonly grantPrincipal: iam.IPrincipal;

  /**
   * The Spark UI logs location if Spark UI monitoring and debugging is enabled.
   *
   * @see https://docs.aws.amazon.com/glue/latest/dg/monitor-spark-ui-jobs.html
   * @see https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html
   */

  constructor(scope: constructs.Construct, id: string, props: ScheduledTriggerProps) {
    super(scope, id, {
      physicalName: props.triggerName,
    });


    // this.role = props.role ?? new iam.Role(this, 'ServiceRole', {
    //   assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
    //   managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole')],
    // });
    // this.grantPrincipal = this.role;


    const triggerResource = new CfnTrigger(this, 'Resource', {
      workflowName: props.workflowName,
      tags: props.tags,
      startOnCreation: props.startOnCreation,
      schedule: props.schedule?.expressionString,
      name: props.triggerName,
      actions: props.actions,
      description: props.description,
      type: TriggerType.SCHEDULED,
    });

    const resourceName = this.getResourceNameAttribute(triggerResource.ref);
    this.triggerArn = triggeerArn(this, resourceName);
    this.triggerName = resourceName;
  }
}


/**
 * Returns the job arn
 * @param scope
 * @param jobName
 */
function triggeerArn(scope: constructs.Construct, jobName: string) : string {
  return cdk.Stack.of(scope).formatArn({
    service: 'glue',
    resource: 'trigger',
    resourceName: jobName,
  });
}
