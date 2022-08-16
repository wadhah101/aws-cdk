import * as cloudwatch from '@aws-cdk/aws-cloudwatch';
import * as events from '@aws-cdk/aws-events';
import * as iam from '@aws-cdk/aws-iam';
import * as logs from '@aws-cdk/aws-logs';
import * as s3 from '@aws-cdk/aws-s3';
import * as cdk from '@aws-cdk/core';
import * as constructs from 'constructs';
import { Code, JobExecutable, JobExecutableConfig, JobType } from '.';
import { IConnection } from './connection';
import { CfnJob, CfnTrigger } from './glue.generated';
import { ISecurityConfiguration } from './security-configuration';
/**
 * The type of predefined worker that is allocated when a job runs.
 *
 * If you need to use a WorkerType that doesn't exist as a static member, you
 * can instantiate a `WorkerType` object, e.g: `WorkerType.of('other type')`.
 */
export class TriggerType {
  /**
   * TODO
   */
  public static readonly Scheduled = new TriggerType('SCHEDULED');

  /**
   * TODO
   */
  public static readonly JobEvents = new TriggerType('JOB_EVENTS');

  /**
   * TODO
   */
  public static readonly onDemand = new TriggerType('ON_DEMAND');


  /**
   * TODO
   */
  public static readonly conditional = new TriggerType('CONDITIONAL')

  /**
   * The name of this WorkerType, as expected by Job resource.
   */
  public readonly name: string;

  private constructor(name: string) {
    this.name = name;
  }
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
}


/**
 * Interface representing a created or an imported {@link Job}.
 */
export interface ITrigger extends cdk.IResource {
  /**
   * The name of the job.
   * @attribute
   */
  readonly triggerName: string;

  /**
   * The ARN of the job.
   * @attribute
   */
  readonly triggerArn: string;

}

abstract class TriggerBase extends cdk.Resource implements ITrigger {
  public abstract readonly triggerArn: string;
  public abstract readonly triggerName: string;
  public abstract readonly grantPrincipal: iam.IPrincipal;
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

/**
 * Construction properties for {@link Job}.
 */
export interface TriggerProps {

  readonly actions: any[]

  readonly description: string,

  readonly predicate: any

  readonly schedule: events.Schedule,

  readonly startOnCreation: boolean

  readonly type: TriggerType,

  readonly workflowName: string

  /**
   * The name of the job.
   *
   * @default - a name is automatically generated
   */
  readonly triggerName?: string;


  readonly tags?: { [key: string]: string };

  /**
   * The IAM role assumed by Glue to run this job.
   *
   * If providing a custom role, it needs to trust the Glue service principal (glue.amazonaws.com) and be granted sufficient permissions.
   *
   * @see https://docs.aws.amazon.com/glue/latest/dg/getting-started-access.html
   *
   * @default - a role is automatically generated
   */
  readonly role?: iam.IRole;
}

/**
 * A Glue Job.
 */
export class Trigger extends TriggerBase {
  // /**
  //  * Creates a Glue Job
  //  *
  //  * @param scope The scope creating construct (usually `this`).
  //  * @param id The construct's id.
  //  * @param attrs Import attributes
  //  */
  // public static fromTriggerAttributes(scope: constructs.Construct, id: string, attrs: TriggerAttributes): ITrigger {
  //   class Import extends TriggerBase {
  //     public readonly triggerName = attrs.jobName;
  //     public readonly triggerArn = 'TODO';
  //     // jobArn(scope, attrs.jobName);
  //     public readonly grantPrincipal = attrs.role ?? new iam.UnknownPrincipal({ resource: this });
  //   }

  //   return new Import(scope, id);
  // }

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
  public readonly role: iam.IRole;

  /**
   * The principal this Glue Job is running as.
   */
  public readonly grantPrincipal: iam.IPrincipal;

  /**
   * The Spark UI logs location if Spark UI monitoring and debugging is enabled.
   *
   * @see https://docs.aws.amazon.com/glue/latest/dg/monitor-spark-ui-jobs.html
   * @see https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html
   */

  constructor(scope: constructs.Construct, id: string, props: TriggerProps) {
    super(scope, id, {
      physicalName: props.triggerName,
    });


    this.role = props.role ?? new iam.Role(this, 'ServiceRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole')],
    });
    this.grantPrincipal = this.role;


    const triggerResource = new CfnTrigger(this, 'Resource', {
      workflowName: props.workflowName,
      tags: props.tags,
      startOnCreation: props.startOnCreation,
      schedule: props.schedule.expressionString,
      predicate: props.predicate,
      name: props.triggerName,
      actions: props.actions,
      description: props.description,
      type: props.type.name,
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
