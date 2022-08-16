import * as cloudwatch from '@aws-cdk/aws-cloudwatch';
import * as events from '@aws-cdk/aws-events';
import * as iam from '@aws-cdk/aws-iam';
import * as logs from '@aws-cdk/aws-logs';
import * as s3 from '@aws-cdk/aws-s3';
import * as cdk from '@aws-cdk/core';
import * as constructs from 'constructs';
import { Code, JobExecutable, JobExecutableConfig, JobType } from '.';
import { IConnection } from './connection';
import { CfnJob ,CfnTrigger } from './glue.generated';
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

export enum triggerState {
  /**
   * State indicating job run succeeded
   */
  SUCCEEDED = 'SUCCEEDED',

  /**
   * State indicating job run failed
   */
  FAILED = 'FAILED',

  /**
   * State indicating job run timed out
   */
  TIMEOUT = 'TIMEOUT',

  /**
   * State indicating job is starting
   */
  STARTING = 'STARTING',

  /**
   * State indicating job is running
   */
  RUNNING = 'RUNNING',

  /**
   * State indicating job is stopping
   */
  STOPPING = 'STOPPING',

  /**
   * State indicating job stopped
   */
  STOPPED = 'STOPPED',
}


/**
 * Interface representing a created or an imported {@link Job}.
 */
export interface ITrigger extends cdk.IResource, iam.IGrantable {
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

  /**
   * Defines a CloudWatch event rule triggered when something happens with this job.
   *
   * @see https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/EventTypes.html#glue-event-types
   */
  onEvent(id: string, options?: events.OnEventOptions): events.Rule;

  /**
   * Defines a CloudWatch event rule triggered when this job moves to the input jobState.
   *
   * @see https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/EventTypes.html#glue-event-types
   */
  onStateChange(id: string, jobState: triggerState, options?: events.OnEventOptions): events.Rule;

  /**
   * Defines a CloudWatch event rule triggered when this job moves to the SUCCEEDED state.
   *
   * @see https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/EventTypes.html#glue-event-types
   */
  onSuccess(id: string, options?: events.OnEventOptions): events.Rule;

  /**
   * Defines a CloudWatch event rule triggered when this job moves to the FAILED state.
   *
   * @see https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/EventTypes.html#glue-event-types
   */
  onFailure(id: string, options?: events.OnEventOptions): events.Rule;

  /**
   * Defines a CloudWatch event rule triggered when this job moves to the TIMEOUT state.
   *
   * @see https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/EventTypes.html#glue-event-types
   */
  onTimeout(id: string, options?: events.OnEventOptions): events.Rule;

  /**
   * Create a CloudWatch metric.
   *
   * @param metricName name of the metric typically prefixed with `glue.driver.`, `glue.<executorId>.` or `glue.ALL.`.
   * @param type the metric type.
   * @param props metric options.
   *
   * @see https://docs.aws.amazon.com/glue/latest/dg/monitoring-awsglue-with-cloudwatch-metrics.html
   */
  metric(metricName: string, type: MetricType, props?: cloudwatch.MetricOptions): cloudwatch.Metric;

  /**
   * Create a CloudWatch Metric indicating job success.
   */
  metricSuccess(props?: cloudwatch.MetricOptions): cloudwatch.Metric;

  /**
   * Create a CloudWatch Metric indicating job failure.
   */
  metricFailure(props?: cloudwatch.MetricOptions): cloudwatch.Metric;

  /**
   * Create a CloudWatch Metric indicating job timeout.
   */
  metricTimeout(props?: cloudwatch.MetricOptions): cloudwatch.Metric;
}

abstract class TriggerBase extends cdk.Resource implements IJob {

  public abstract readonly jobArn: string;
  public abstract readonly jobName: string;
  public abstract readonly grantPrincipal: iam.IPrincipal;

  /**
   * Create a CloudWatch Event Rule for this Glue Job when it's in a given state
   *
   * @param id construct id
   * @param options event options. Note that some values are overridden if provided, these are
   *  - eventPattern.source = ['aws.glue']
   *  - eventPattern.detailType = ['Glue Job State Change', 'Glue Job Run Status']
   *  - eventPattern.detail.jobName = [this.jobName]
   *
   * @see https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/EventTypes.html#glue-event-types
   */
  public onEvent(id: string, options: events.OnEventOptions = {}): events.Rule {
    const rule = new events.Rule(this, id, options);
    rule.addTarget(options.target);
    rule.addEventPattern({
      source: ['aws.glue'],
      detailType: ['Glue Job State Change', 'Glue Job Run Status'],
      detail: {
        jobName: [this.jobName],
      },
    });
    return rule;
  }

  /**
   * Create a CloudWatch Event Rule for the transition into the input jobState.
   *
   * @param id construct id.
   * @param jobState the job state.
   * @param options optional event options.
   */
  public onStateChange(id: string, jobState: triggerState, options: events.OnEventOptions = {}): events.Rule {
    const rule = this.onEvent(id, {
      description: `Rule triggered when Glue job ${this.jobName} is in ${jobState} state`,
      ...options,
    });
    rule.addEventPattern({
      detail: {
        state: [jobState],
      },
    });
    return rule;
  }

  /**
   * Create a CloudWatch Event Rule matching JobState.SUCCEEDED.
   *
   * @param id construct id.
   * @param options optional event options. default is {}.
   */
  public onSuccess(id: string, options: events.OnEventOptions = {}): events.Rule {
    return this.onStateChange(id, triggerState.SUCCEEDED, options);
  }

  /**
   * Return a CloudWatch Event Rule matching FAILED state.
   *
   * @param id construct id.
   * @param options optional event options. default is {}.
   */
  public onFailure(id: string, options: events.OnEventOptions = {}): events.Rule {
    return this.onStateChange(id, triggerState.FAILED, options);
  }

  /**
   * Return a CloudWatch Event Rule matching TIMEOUT state.
   *
   * @param id construct id.
   * @param options optional event options. default is {}.
   */
  public onTimeout(id: string, options: events.OnEventOptions = {}): events.Rule {
    return this.onStateChange(id, triggerState.TIMEOUT, options);
  }

  /**
   * Create a CloudWatch metric.
   *
   * @param metricName name of the metric typically prefixed with `glue.driver.`, `glue.<executorId>.` or `glue.ALL.`.
   * @param type the metric type.
   * @param props metric options.
   *
   * @see https://docs.aws.amazon.com/glue/latest/dg/monitoring-awsglue-with-cloudwatch-metrics.html
   */
  public metric(metricName: string, type: MetricType, props?: cloudwatch.MetricOptions): cloudwatch.Metric {
    return new cloudwatch.Metric({
      metricName,
      namespace: 'Glue',
      dimensionsMap: {
        JobName: this.jobName,
        JobRunId: 'ALL',
        Type: type,
      },
      ...props,
    }).attachTo(this);
  }

  /**
   * Return a CloudWatch Metric indicating job success.
   *
   * This metric is based on the Rule returned by no-args onSuccess() call.
   */
  public metricSuccess(props?: cloudwatch.MetricOptions): cloudwatch.Metric {
    return metricRule(this.metricJobStateRule('SuccessMetricRule', triggerState.SUCCEEDED), props);
  }

  /**
   * Return a CloudWatch Metric indicating job failure.
   *
   * This metric is based on the Rule returned by no-args onFailure() call.
   */
  public metricFailure(props?: cloudwatch.MetricOptions): cloudwatch.Metric {
    return metricRule(this.metricJobStateRule('FailureMetricRule', triggerState.FAILED), props);
  }

  /**
   * Return a CloudWatch Metric indicating job timeout.
   *
   * This metric is based on the Rule returned by no-args onTimeout() call.
   */
  public metricTimeout(props?: cloudwatch.MetricOptions): cloudwatch.Metric {
    return metricRule(this.metricJobStateRule('TimeoutMetricRule', triggerState.TIMEOUT), props);
  }

  /**
   * Creates or retrieves a singleton event rule for the input job state for use with the metric JobState methods.
   *
   * @param id construct id.
   * @param jobState the job state.
   * @private
   */
  private metricJobStateRule(id: string, jobState: triggerState): events.Rule {
    return this.node.tryFindChild(id) as events.Rule ?? this.onStateChange(id, jobState);
  }
}

/**
 * Attributes for importing {@link Job}.
 */
export interface TriggerAttributes {
  /**
   * The name of the job.
   */
  readonly jobName: string;

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
