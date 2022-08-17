import { Template } from '@aws-cdk/assertions';
import * as cdk from '@aws-cdk/core';
import * as glue from '../lib';
import { TriggerType } from '../lib';


describe('Trigger', () => {
  const triggerName = 'test-trigger';
  let stack: cdk.Stack;
  let trigger: glue.Trigger;
  let defaultProps: glue.TriggerProps;

  beforeEach(() => {
    stack = new cdk.Stack();
  });


  describe('new', () => {


    beforeEach(() => {
      defaultProps = {
        actions: [],
        workflowName: '',
        type: TriggerType.ON_DEMAND,
      };
    });

    describe('with necessary props only', () => {
      beforeEach(() => {
        trigger = new glue.Trigger(stack, 'Trigger', defaultProps);
      });


      test('should return correct triggerName and tirggerArn from CloudFormation', () => {
        expect(stack.resolve(trigger.triggerName)).toEqual({ Ref: 'TriggerD50EE54C' });
        expect(stack.resolve(trigger.triggerArn)).toEqual({
          'Fn::Join': ['', ['arn:', { Ref: 'AWS::Partition' }, ':glue:', { Ref: 'AWS::Region' }, ':', { Ref: 'AWS::AccountId' }, ':trigger/', { Ref: 'TriggerD50EE54C' }]],
        });
      });

      test('with a custom triggerName should set it in CloudFormation', () => {
        trigger = new glue.Trigger(stack, 'triggerWithName', {
          ...defaultProps,
          triggerName,
        });

        Template.fromStack(stack).hasResourceProperties('AWS::Glue::Trigger', {
          Name: triggerName,
        });
      });

      // eslint-disable-next-line jest/no-commented-out-tests
      //   test('with a custom jobName should set it in CloudFormation', () => {
      //     job = new glue.Job(stack, 'JobWithName', {
      //       ...defaultProps,
      //       jobName: triggerName,
      //     });

      //     Template.fromStack(stack).hasResourceProperties('AWS::Glue::Job', {
      //       Name: triggerName,
      //     });
      //   });
      // });


      // eslint-disable-next-line jest/no-commented-out-tests
      // describe('with extended props', () => {
      //   beforeEach(() => {
      //     job = new glue.Job(stack, 'Job', {
      //       ...defaultProps,
      //       jobName,
      //       description: 'test job',
      //       workerType: glue.WorkerType.G_2X,
      //       workerCount: 10,
      //       maxConcurrentRuns: 2,
      //       maxRetries: 2,
      //       timeout: cdk.Duration.minutes(5),
      //       notifyDelayAfter: cdk.Duration.minutes(1),
      //       defaultArguments: {
      //         arg1: 'value1',
      //         arg2: 'value2',
      //       },
      //       connections: [glue.Connection.fromConnectionName(stack, 'ImportedConnection', 'ConnectionName')],
      //       securityConfiguration: glue.SecurityConfiguration.fromSecurityConfigurationName(stack, 'ImportedSecurityConfiguration', 'SecurityConfigurationName'),
      //       enableProfilingMetrics: true,
      //       tags: {
      //         key: 'value',
      //       },
      //     });
      //   });


    });
  });
});