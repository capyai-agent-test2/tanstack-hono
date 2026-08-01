import * as path from "node:path";

import * as cdk from "aws-cdk-lib";
import * as cloudwatch from "aws-cdk-lib/aws-cloudwatch";
import * as cloudwatchActions from "aws-cdk-lib/aws-cloudwatch-actions";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as lambdaNodejs from "aws-cdk-lib/aws-lambda-nodejs";
import * as logs from "aws-cdk-lib/aws-logs";
import * as sns from "aws-cdk-lib/aws-sns";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import { Construct } from "constructs";

import { opsAlertsTopicArn } from "./ops-alerts";

export type TemporalCapacityControllerEnvName = "dev" | "prod" | "staging";

export interface TemporalCapacityControllerEnvStackProps
  extends cdk.StackProps {
  envName: TemporalCapacityControllerEnvName;
  // Static vCPU partition for this environment (values live in
  // @capy/env-map). Both reconcile allocation and deploy-reservation
  // admission clamp to it inside the controller.
  vcpuBudget: number;
  // False until the prod handoff: resources deploy but the reconcile chain
  // never bootstraps (health rule disabled) and alarms do not page, so the
  // legacy shared controller stays the only writer. Flipping this to true is
  // the per-environment cutover switch.
  chainEnabled: boolean;
  // Seconds between reconcile cycles. Every cycle runs an account-wide
  // Fargate inventory scan, and the account's ECS read budget is shared:
  // three chains at the legacy 20s cadence would out-scan the 10s-cycle
  // incident rate that starved deploy-time invokes (gate-2 F2). Stagger
  // per env: prod keeps 20s, dev/staging run slower.
  cycleWaitSeconds: number;
}

// Per-environment capacity controller (env-split program): one controller
// instance per environment with its own Lambda, Step Functions chain,
// DynamoDB tables, and IAM scoped to exactly that environment's cluster.
// The environment partition comes from CAPACITY_ENVIRONMENT +
// ENVIRONMENT_VCPU_BUDGET; the reconcile core is unchanged.
//
// This stack also owns the canonical Environment-dimension queue-backlog
// alarms (admission/dispatch liveness, backlog age, telemetry-absent) and the
// dev SupersededBuildGenerations alarm (decommission P4). Their names collide
// one-for-one with the legacy shared stack's alarms, and CloudFormation
// PutMetricAlarm silently adopts a same-named alarm: deploying these while
// the legacy stack still exists would let the legacy stack's deletion (P5)
// take the alarms with it. Deploy order is therefore legacy deletion first —
// the interim CLI bridge clones (suffix -bridge) hold coverage in between and
// retire once these are live (P6).
export class TemporalCapacityControllerEnvStack extends cdk.Stack {
  constructor(
    scope: Construct,
    id: string,
    props: TemporalCapacityControllerEnvStackProps,
  ) {
    super(scope, id, props);
    const { envName, vcpuBudget, chainEnabled, cycleWaitSeconds } = props;

    const table = new dynamodb.Table(this, "ControlTable", {
      tableName: `capy-temporal-capacity-control-${envName}`,
      partitionKey: { name: "PK", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "SK", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      encryption: dynamodb.TableEncryption.AWS_MANAGED,
      pointInTimeRecoverySpecification: {
        pointInTimeRecoveryEnabled: true,
      },
      timeToLiveAttribute: "ttl",
      deletionProtection: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });
    const drainTable = new dynamodb.Table(this, "DrainTable", {
      tableName: `capy-temporal-worker-drains-${envName}`,
      partitionKey: { name: "PK", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "SK", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      encryption: dynamodb.TableEncryption.AWS_MANAGED,
      pointInTimeRecoverySpecification: {
        pointInTimeRecoveryEnabled: true,
      },
      timeToLiveAttribute: "ttl",
      deletionProtection: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const logGroup = new logs.LogGroup(this, "ControllerLogGroup", {
      logGroupName: `/aws/lambda/capy-temporal-capacity-controller-${envName}`,
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const bundling = {
      format: lambdaNodejs.OutputFormat.ESM,
      target: "node22",
      minify: true,
      sourceMap: true,
      mainFields: ["module", "main"],
      externalModules: [],
      // CJS deps in the ESM bundle call require() at init; without this
      // shim the runtime dies with "Dynamic require of ... is not supported".
      banner:
        'import { createRequire } from "node:module"; const require = createRequire(import.meta.url);',
    };
    const controllerEnvironment = {
      AWS_ACCOUNT_ID: this.account,
      CAPACITY_CONTROL_TABLE_NAME: table.tableName,
      CAPACITY_DRAIN_TABLE_NAME: drainTable.tableName,
      CAPACITY_ENVIRONMENT: envName,
      ENVIRONMENT_VCPU_BUDGET: String(vcpuBudget),
      HARD_RESERVE_VCPU: "96",
      // WORKER_HEARTBEAT_FRESHNESS_MS is deliberately NOT pinned here: the
      // config default (90s, see config.ts) governs. The old 15s pin sampled
      // only a small, biased subset of workers each cycle (ListWorkers
      // visibility ages run to p95 ~55s), which made slot-utilization
      // readings flap and perpetually reset scale-in eligibility.
      // Must exceed the controller Lambda timeout so a timed-out cycle
      // cannot outlive its own lock.
      RECONCILER_LOCK_MS: "190000",
      CONTROLLER_CYCLE_STALE_MS: "240000",
      RESERVATION_SNAPSHOT_MAX_AGE_MS: "240000",
      CHAIN_ROTATION_CYCLES: "2000",
    };

    const controller = new lambdaNodejs.NodejsFunction(this, "Controller", {
      functionName: `capy-temporal-capacity-controller-${envName}`,
      entry: path.join(
        __dirname,
        "../../temporal-capacity-controller/src/handler.ts",
      ),
      handler: "handler",
      runtime: lambda.Runtime.NODEJS_22_X,
      architecture: lambda.Architecture.ARM_64,
      memorySize: 512,
      // Scoped instances keep the legacy timeout: even one environment's
      // reconcile runs the account-wide Fargate inventory for quota
      // accounting, which is the slow path the 180s covers.
      timeout: cdk.Duration.seconds(180),
      reservedConcurrentExecutions: 3,
      logGroup,
      depsLockFilePath: path.join(__dirname, "../../../pnpm-lock.yaml"),
      projectRoot: path.join(__dirname, "../../.."),
      bundling,
      environment: controllerEnvironment,
    });
    table.grantReadWriteData(controller);
    drainTable.grantReadWriteData(controller);

    const loadGate = new lambdaNodejs.NodejsFunction(this, "LoadGate", {
      functionName: `capy-temporal-capacity-load-gate-${envName}`,
      entry: path.join(
        __dirname,
        "../../temporal-capacity-controller/src/load-gate-handler.ts",
      ),
      handler: "handler",
      runtime: lambda.Runtime.NODEJS_22_X,
      architecture: lambda.Architecture.ARM_64,
      memorySize: 512,
      timeout: cdk.Duration.seconds(20),
      reservedConcurrentExecutions: 4,
      depsLockFilePath: path.join(__dirname, "../../../pnpm-lock.yaml"),
      projectRoot: path.join(__dirname, "../../.."),
      bundling,
      environment: controllerEnvironment,
    });
    table.grantReadData(loadGate);

    const readCapacityInputs = new iam.PolicyStatement({
      sid: "ReadCapacityInputs",
      actions: [
        "ecs:DescribeServices",
        "ecs:DescribeTasks",
        "ecs:DescribeTaskDefinition",
        "ecs:GetTaskProtection",
        "ecs:ListClusters",
        "ecs:ListServices",
        "ecs:ListTasks",
        "servicequotas:GetServiceQuota",
        "cloudwatch:GetMetricData",
        "cloudwatch:PutMetricData",
      ],
      resources: ["*"],
    });
    controller.addToRolePolicy(readCapacityInputs);
    loadGate.addToRolePolicy(readCapacityInputs);

    // Mutation rights are the environment boundary: this controller can only
    // actuate its own environment's cluster, tasks, SSM credentials, and
    // worker roles.
    controller.addToRolePolicy(
      new iam.PolicyStatement({
        sid: "UpdateManagedTemporalDesiredCounts",
        actions: ["ecs:UpdateService"],
        resources: [
          `arn:${this.partition}:ecs:${this.region}:${this.account}:service/capy-temporal-worker-${envName}/capy-temporal-worker-${envName}-*`,
        ],
      }),
    );
    controller.addToRolePolicy(
      new iam.PolicyStatement({
        sid: "UpdateManagedTemporalTaskProtection",
        actions: ["ecs:UpdateTaskProtection"],
        resources: [
          `arn:${this.partition}:ecs:${this.region}:${this.account}:task/capy-temporal-worker-${envName}/*`,
        ],
      }),
    );
    controller.addToRolePolicy(
      new iam.PolicyStatement({
        sid: "PassTemporalWorkerRoles",
        actions: ["iam:PassRole"],
        resources: [
          `arn:${this.partition}:iam::${this.account}:role/capy-temporal-worker-${envName}-task-role`,
          `arn:${this.partition}:iam::${this.account}:role/capy-temporal-worker-${envName}-execution-role`,
        ],
        conditions: {
          StringEquals: {
            "iam:PassedToService": "ecs-tasks.amazonaws.com",
          },
        },
      }),
    );
    const readTemporalCredentials = new iam.PolicyStatement({
      sid: "ReadTemporalCredentials",
      actions: ["ssm:GetParameters"],
      resources: [
        `arn:${this.partition}:ssm:${this.region}:${this.account}:parameter/capy/${envName}/env/TEMPORAL_*`,
      ],
    });
    const decryptTemporalCredentials = new iam.PolicyStatement({
      sid: "DecryptTemporalCredentials",
      actions: ["kms:Decrypt"],
      resources: ["*"],
      conditions: {
        StringEquals: {
          "kms:ViaService": `ssm.${this.region}.amazonaws.com`,
        },
      },
    });
    controller.addToRolePolicy(readTemporalCredentials);
    controller.addToRolePolicy(decryptTemporalCredentials);
    loadGate.addToRolePolicy(readTemporalCredentials);
    loadGate.addToRolePolicy(decryptTemporalCredentials);

    const stateMachineName = `capy-temporal-capacity-controller-${envName}`;
    const stateMachineArn = `arn:${this.partition}:states:${this.region}:${this.account}:stateMachine:${stateMachineName}`;
    controller.addToRolePolicy(
      new iam.PolicyStatement({
        sid: "ManageControllerChain",
        actions: [
          "states:StartExecution",
          "states:DescribeExecution",
          "states:ListExecutions",
        ],
        resources: [
          stateMachineArn,
          `arn:${this.partition}:states:${this.region}:${this.account}:execution:${stateMachineName}:*`,
        ],
      }),
    );

    const stateMachineRole = new iam.Role(this, "StateMachineRole", {
      assumedBy: new iam.ServicePrincipal("states.amazonaws.com"),
    });
    controller.grantInvoke(stateMachineRole);
    const stateMachine = new sfn.CfnStateMachine(this, "StateMachine", {
      stateMachineName,
      stateMachineType: "STANDARD",
      roleArn: stateMachineRole.roleArn,
      definitionString: JSON.stringify({
        StartAt: "Wait",
        States: {
          Wait: { Type: "Wait", Seconds: cycleWaitSeconds, Next: "Reconcile" },
          Reconcile: {
            Type: "Task",
            Resource: "arn:aws:states:::lambda:invoke",
            Parameters: {
              FunctionName: "${ControllerLambdaArn}",
              Payload: {
                operation: "reconcile",
                "stateMachineArn.$": "$$.StateMachine.Id",
                "executionArn.$": "$$.Execution.Id",
                "chainGeneration.$": "$.chainGeneration",
                "cycleIndex.$": "$.cycleIndex",
              },
            },
            OutputPath: "$.Payload",
            Retry: [
              {
                ErrorEquals: [
                  "Lambda.ServiceException",
                  "Lambda.AWSLambdaException",
                  "Lambda.SdkClientException",
                  "Lambda.TooManyRequestsException",
                ],
                IntervalSeconds: 2,
                BackoffRate: 2,
                MaxAttempts: 2,
              },
            ],
            Catch: [
              {
                ErrorEquals: ["States.ALL"],
                ResultPath: "$.lastError",
                Next: "Recover",
              },
            ],
            Next: "ShouldRotate",
          },
          Recover: {
            Type: "Wait",
            Seconds: cycleWaitSeconds,
            Next: "Reconcile",
          },
          ShouldRotate: {
            Type: "Choice",
            Choices: [
              // A superseded execution (its fenced chain heartbeat lost to a
              // successor) must end deterministically instead of looping
              // forever. IsPresent guards outputs from Lambda versions that
              // predate the terminate field.
              {
                And: [
                  { Variable: "$.terminate", IsPresent: true },
                  { Variable: "$.terminate", BooleanEquals: true },
                ],
                Next: "Done",
              },
              {
                Variable: "$.rotate",
                BooleanEquals: true,
                Next: "StartSuccessor",
              },
            ],
            Default: "Wait",
          },
          StartSuccessor: {
            Type: "Task",
            Resource: "arn:aws:states:::lambda:invoke",
            Parameters: {
              FunctionName: "${ControllerLambdaArn}",
              Payload: {
                operation: "rotate-chain",
                "stateMachineArn.$": "$$.StateMachine.Id",
                "chainGeneration.$": "$.chainGeneration",
              },
            },
            Retry: [
              {
                ErrorEquals: ["States.ALL"],
                IntervalSeconds: 2,
                BackoffRate: 2,
                MaxAttempts: 5,
              },
            ],
            Catch: [
              {
                ErrorEquals: ["States.ALL"],
                ResultPath: "$.lastRotationError",
                Next: "Done",
              },
            ],
            Next: "Done",
          },
          Done: { Type: "Succeed" },
        },
      }),
      definitionSubstitutions: {
        ControllerLambdaArn: controller.functionArn,
      },
    });

    const bootstrapDlq = new sqs.Queue(this, "BootstrapDeadLetterQueue", {
      queueName: `capy-temporal-capacity-controller-bootstrap-dlq-${envName}`,
      retentionPeriod: cdk.Duration.days(14),
      encryption: sqs.QueueEncryption.SQS_MANAGED,
    });
    new events.Rule(this, "ControllerHealthSchedule", {
      ruleName: `capy-temporal-capacity-controller-health-${envName}`,
      description: `Ensure exactly one durable ${envName} Temporal capacity controller chain is running`,
      schedule: events.Schedule.rate(cdk.Duration.minutes(1)),
      // The cutover switch: while disabled, no chain ever bootstraps and this
      // controller performs no writes — the legacy shared controller remains
      // the single writer for the environment.
      enabled: chainEnabled,
      targets: [
        new targets.LambdaFunction(controller, {
          event: events.RuleTargetInput.fromObject({
            operation: "ensure-chain",
            stateMachineArn: stateMachine.attrArn,
          }),
          deadLetterQueue: bootstrapDlq,
          retryAttempts: 2,
          maxEventAge: cdk.Duration.minutes(5),
        }),
      ],
    });

    // Alarm actions reference the shared ops topic by ARN; ownership stays
    // with the legacy controller stack until it is decommissioned.
    const opsAlertsTopic = sns.Topic.fromTopicArn(
      this,
      "OpsAlertsTopic",
      opsAlertsTopicArn(this),
    );
    const opsAlertsAction = new cloudwatchActions.SnsAction(opsAlertsTopic);
    const controllerDimensions = { Controller: envName };

    const alarms: cloudwatch.Alarm[] = [];
    const lambdaErrors = controller.metricErrors({
      period: cdk.Duration.minutes(1),
      statistic: "Sum",
    });
    const lambdaThrottles = controller.metricThrottles({
      period: cdk.Duration.minutes(1),
      statistic: "Sum",
    });
    alarms.push(
      new cloudwatch.Alarm(this, "ControllerErrors", {
        alarmName: `capy-temporal-capacity-controller-errors-${envName}`,
        metric: lambdaErrors,
        threshold: 1,
        evaluationPeriods: 1,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      }),
    );
    alarms.push(
      new cloudwatch.Alarm(this, "ControllerThrottles", {
        alarmName: `capy-temporal-capacity-controller-throttles-${envName}`,
        metric: lambdaThrottles,
        threshold: 1,
        evaluationPeriods: 1,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      }),
    );
    alarms.push(
      new cloudwatch.Alarm(this, "ControllerErrorsSustained", {
        alarmName: `capy-temporal-capacity-controller-errors-sustained-${envName}`,
        metric: controller.metricErrors({
          period: cdk.Duration.minutes(15),
          statistic: "Sum",
        }),
        threshold: 3,
        evaluationPeriods: 1,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      }),
    );
    alarms.push(
      new cloudwatch.Alarm(this, "ControllerCycleStale", {
        alarmName: `capy-temporal-capacity-controller-cycle-stale-${envName}`,
        metric: new cloudwatch.Metric({
          namespace: "Capy/TemporalCapacity",
          metricName: "CycleCompleted",
          dimensionsMap: controllerDimensions,
          // Must comfortably exceed the reconcile Wait (20s) plus cycle
          // duration (Lambda timeout 180s), or healthy chains alarm as stale.
          period: cdk.Duration.seconds(300),
          statistic: "Sum",
        }),
        threshold: 1,
        comparisonOperator: cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD,
        evaluationPeriods: 1,
        treatMissingData: cloudwatch.TreatMissingData.BREACHING,
      }),
    );
    alarms.push(
      new cloudwatch.Alarm(this, "ControllerPartialCycle", {
        alarmName: `capy-temporal-capacity-controller-partial-cycle-${envName}`,
        metric: new cloudwatch.Metric({
          namespace: "Capy/TemporalCapacity",
          metricName: "CycleResult",
          dimensionsMap: { Result: "PARTIAL", ...controllerDimensions },
          period: cdk.Duration.minutes(1),
          statistic: "Sum",
        }),
        threshold: 1,
        evaluationPeriods: 1,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      }),
    );
    if (envName === "prod") {
      alarms.push(
        new cloudwatch.Alarm(this, "ControllerUngrantedProd", {
          alarmName: `capy-temporal-capacity-controller-ungranted-prod-${envName}`,
          metric: new cloudwatch.Metric({
            namespace: "Capy/TemporalCapacity",
            metricName: "UngrantedProdReplicas",
            dimensionsMap: controllerDimensions,
            period: cdk.Duration.seconds(30),
            statistic: "Maximum",
          }),
          threshold: 1,
          evaluationPeriods: 1,
          treatMissingData: cloudwatch.TreatMissingData.BREACHING,
        }),
      );
      // Account-quota alarms are account-scoped; exactly one scoped instance
      // (prod) owns them so three controllers do not triple-page.
      for (const threshold of [70, 80, 90]) {
        alarms.push(
          new cloudwatch.Alarm(this, `FargateQuota${threshold}`, {
            alarmName: `capy-temporal-capacity-fargate-${threshold}-${envName}`,
            metric: new cloudwatch.Metric({
              namespace: "Capy/TemporalCapacity",
              metricName: "FargateQuotaUtilization",
              dimensionsMap: controllerDimensions,
              period: cdk.Duration.minutes(1),
              statistic: "Maximum",
            }),
            threshold,
            evaluationPeriods: 1,
            treatMissingData: cloudwatch.TreatMissingData.BREACHING,
          }),
        );
      }
    }
    for (const metricName of [
      "StaleTemporalInputs",
      "StaleWorkerHeartbeats",
      "LoadGateDenied",
    ]) {
      alarms.push(
        new cloudwatch.Alarm(this, `${metricName}Alarm`, {
          alarmName: `capy-temporal-capacity-${metricName
            .replaceAll(/([a-z])([A-Z])/g, "$1-$2")
            .toLowerCase()}-${envName}`,
          metric: new cloudwatch.Metric({
            namespace: "Capy/TemporalCapacity",
            metricName,
            dimensionsMap: controllerDimensions,
            period: cdk.Duration.seconds(30),
            statistic: "Maximum",
          }),
          threshold: 1,
          evaluationPeriods: 1,
          treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
        }),
      );
    }
    for (const metricName of ["PendingTasks", "DesiredNotReadyReplicas"]) {
      alarms.push(
        new cloudwatch.Alarm(this, `${metricName}SustainedAlarm`, {
          alarmName: `capy-temporal-capacity-${metricName
            .replaceAll(/([a-z])([A-Z])/g, "$1-$2")
            .toLowerCase()}-sustained-${envName}`,
          metric: new cloudwatch.Metric({
            namespace: "Capy/TemporalCapacity",
            metricName,
            dimensionsMap: controllerDimensions,
            period: cdk.Duration.minutes(1),
            statistic: "Maximum",
          }),
          threshold: 1,
          evaluationPeriods: 5,
          treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
        }),
      );
    }
    const runningControllerChains = new cloudwatch.Metric({
      namespace: "Capy/TemporalCapacity",
      metricName: "RunningControllerChains",
      dimensionsMap: controllerDimensions,
      period: cdk.Duration.minutes(1),
      statistic: "Maximum",
    });
    alarms.push(
      new cloudwatch.Alarm(this, "ControllerChainMissing", {
        alarmName: `capy-temporal-capacity-controller-chain-missing-${envName}`,
        metric: runningControllerChains,
        threshold: 1,
        comparisonOperator: cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD,
        evaluationPeriods: 2,
        treatMissingData: cloudwatch.TreatMissingData.BREACHING,
      }),
    );
    alarms.push(
      new cloudwatch.Alarm(this, "ControllerChainDuplicate", {
        alarmName: `capy-temporal-capacity-controller-chain-duplicate-${envName}`,
        metric: runningControllerChains,
        threshold: 1,
        comparisonOperator:
          cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
        evaluationPeriods: 1,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      }),
    );
    // The controller emits DrainDeadlineExpired every cycle (zero included),
    // so NOT_BREACHING gaps are safe and any nonzero datapoint must page.
    alarms.push(
      new cloudwatch.Alarm(this, "DrainDeadlineExpired", {
        alarmName: `capy-temporal-capacity-drain-deadline-expired-${envName}`,
        alarmDescription:
          "A protected drain intent was terminally CANCELLED with reason " +
          "DRAIN_DEADLINE_EXPIRED: the drain never completed inside its " +
          "deadline. This failure class ran silent for 19 hours on " +
          "2026-07-15 with no alarm coverage; any occurrence must page.",
        metric: new cloudwatch.Metric({
          namespace: "Capy/TemporalCapacity",
          metricName: "DrainDeadlineExpired",
          dimensionsMap: controllerDimensions,
          period: cdk.Duration.minutes(1),
          statistic: "Sum",
        }),
        threshold: 1,
        evaluationPeriods: 1,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      }),
    );
    // EnvironmentBudgetUtilization lands once per cycle, and dev/staging
    // cycles can run longer than a minute; the period must be wide enough
    // that every bucket holds a datapoint, or NOT_BREACHING gaps would reset
    // the consecutive-period count and the alarm could never reach 3.
    alarms.push(
      new cloudwatch.Alarm(this, "EnvironmentBudget90", {
        alarmName: `capy-temporal-capacity-environment-budget-90-${envName}`,
        metric: new cloudwatch.Metric({
          namespace: "Capy/TemporalCapacity",
          metricName: "EnvironmentBudgetUtilization",
          dimensionsMap: controllerDimensions,
          period: cdk.Duration.minutes(5),
          statistic: "Maximum",
        }),
        threshold: 90,
        evaluationPeriods: 3,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      }),
    );

    // Canonical per-env queue-backlog alarms (decommission P4), ported from
    // the legacy shared stack with thresholds unchanged. Queue-backlog
    // metrics are keyed by Environment (not Controller): the emitting
    // controller partitions them by the environment of the observed queue.
    const environmentDimensions = { Environment: envName };
    alarms.push(
      new cloudwatch.Alarm(this, "AdmissionLiveness", {
        alarmName: `capy-temporal-capacity-admission-liveness-${envName}`,
        metric: new cloudwatch.Metric({
          namespace: "Capy/TemporalCapacity",
          metricName: "StarvedWorkflowQueues",
          dimensionsMap: environmentDimensions,
          period: cdk.Duration.minutes(5),
          statistic: "Minimum",
        }),
        threshold: 1,
        comparisonOperator:
          cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        evaluationPeriods: 2,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      }),
    );
    // Activity-plane twin of AdmissionLiveness: activity queues holding
    // backlog while dispatching nothing.
    alarms.push(
      new cloudwatch.Alarm(this, "ActivityDispatchLiveness", {
        alarmName: `capy-temporal-capacity-activity-dispatch-liveness-${envName}`,
        metric: new cloudwatch.Metric({
          namespace: "Capy/TemporalCapacity",
          metricName: "StarvedActivityQueues",
          dimensionsMap: environmentDimensions,
          period: cdk.Duration.minutes(5),
          statistic: "Minimum",
        }),
        threshold: 1,
        comparisonOperator:
          cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        evaluationPeriods: 2,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      }),
    );
    alarms.push(
      new cloudwatch.Alarm(this, "WorkflowBacklogAge", {
        alarmName: `capy-temporal-capacity-workflow-backlog-age-${envName}`,
        metric: new cloudwatch.Metric({
          namespace: "Capy/TemporalCapacity",
          metricName: "QueueBacklogAgeSeconds",
          dimensionsMap: { ...environmentDimensions, TaskType: "workflow" },
          period: cdk.Duration.minutes(5),
          statistic: "Minimum",
        }),
        threshold: 300,
        comparisonOperator:
          cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
        evaluationPeriods: 2,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      }),
    );
    // Activity backlog age catches slow-but-alive dispatch, which the
    // starved-queue predicate cannot see (dispatch rate > 0). 900s is
    // deliberately looser than the 300s workflow threshold because activity
    // backlogs breathe under load spikes.
    alarms.push(
      new cloudwatch.Alarm(this, "ActivityBacklogAge", {
        alarmName: `capy-temporal-capacity-activity-backlog-age-${envName}`,
        metric: new cloudwatch.Metric({
          namespace: "Capy/TemporalCapacity",
          metricName: "QueueBacklogAgeSeconds",
          dimensionsMap: { ...environmentDimensions, TaskType: "activity" },
          period: cdk.Duration.minutes(5),
          statistic: "Minimum",
        }),
        threshold: 900,
        comparisonOperator:
          cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
        evaluationPeriods: 2,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      }),
    );
    // Every backlog alarm above treats missing data as NOT_BREACHING, so a
    // dead emitter silently disarms them. This alarm inverts that: if
    // QueueBacklogAgeSeconds publishes no datapoint for a (env, task type)
    // pair for 15m, fire via TreatMissingData.BREACHING (SampleCount < 1
    // never has a real datapoint to evaluate).
    for (const taskType of ["workflow", "activity"] as const) {
      const taskTypeSuffix = taskType === "workflow" ? "Wf" : "Act";
      alarms.push(
        new cloudwatch.Alarm(this, `BacklogTelemetryAbsent${taskTypeSuffix}`, {
          alarmName: `capy-temporal-capacity-backlog-telemetry-absent-${taskType}-${envName}`,
          metric: new cloudwatch.Metric({
            namespace: "Capy/TemporalCapacity",
            metricName: "QueueBacklogAgeSeconds",
            dimensionsMap: { ...environmentDimensions, TaskType: taskType },
            period: cdk.Duration.minutes(15),
            statistic: "SampleCount",
          }),
          threshold: 1,
          comparisonOperator: cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD,
          evaluationPeriods: 1,
          treatMissingData: cloudwatch.TreatMissingData.BREACHING,
        }),
      );
    }
    // Layer-2 (warning) fleet-aging alarm, dev only. SupersededBuildGenerations
    // counts distinct DRAINING/DRAINED build generations still managed on dev.
    // A healthy dev carries at most one generation draining behind the current
    // build; a sustained count of 3+ means old per-build worker pools are not
    // retiring — the retirement reaper is wedged or blocked by pinned dev
    // workflows — and the dev fleet is bloating. This is a cause diagnostic
    // that annotates an incident; per #9045's two-layer taxonomy it is
    // warning-tier and must not be treated as a page on its own. Prod and
    // staging are intentionally not alarmed: their reapers are unaffected and
    // they carry no disposable pinned dev jams.
    if (envName === "dev") {
      alarms.push(
        new cloudwatch.Alarm(this, "SupersededBuildGenerations", {
          alarmName: `capy-temporal-capacity-superseded-build-generations-${envName}`,
          alarmDescription:
            "Layer 2 (warning): dev is carrying >=3 superseded worker build generations for 30m; the retirement reaper is wedged or blocked and the dev fleet is bloating. See .github/workflows/cleanup.yml (the iac retire janitor).",
          metric: new cloudwatch.Metric({
            namespace: "Capy/TemporalCapacity",
            metricName: "SupersededBuildGenerations",
            dimensionsMap: environmentDimensions,
            period: cdk.Duration.minutes(5),
            statistic: "Maximum",
          }),
          threshold: 3,
          comparisonOperator:
            cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
          evaluationPeriods: 6,
          datapointsToAlarm: 6,
          treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
        }),
      );
    }

    for (const alarm of alarms) {
      alarm.addAlarmAction(opsAlertsAction);
      // Pre-cutover the chain never runs, so missing-data=BREACHING alarms
      // (cycle-stale, chain-missing, quota) would page on a deliberately idle
      // controller. Paging arms together with the chain.
      if (!chainEnabled) {
        (alarm.node.defaultChild as cloudwatch.CfnAlarm).actionsEnabled = false;
      }
    }

    const dashboard = new cloudwatch.Dashboard(this, "Dashboard", {
      dashboardName: `capy-temporal-capacity-controller-${envName}`,
    });
    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: `Controller Lambda (${envName})`,
        left: [lambdaErrors, lambdaThrottles, controller.metricDuration()],
      }),
    );

    new cdk.CfnOutput(this, "ControlTableName", { value: table.tableName });
    new cdk.CfnOutput(this, "DrainTableName", {
      value: drainTable.tableName,
    });
    new cdk.CfnOutput(this, "ControllerFunctionName", {
      value: controller.functionName,
    });
    new cdk.CfnOutput(this, "LoadGateFunctionName", {
      value: loadGate.functionName,
    });
    new cdk.CfnOutput(this, "ControllerStateMachineArn", {
      value: stateMachine.attrArn,
    });
    new cdk.CfnOutput(this, "EnvironmentVcpuBudget", {
      value: String(vcpuBudget),
    });
  }
}
