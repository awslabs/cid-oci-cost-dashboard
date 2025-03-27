**Introduction to OCI Cloud Intelligence Dashboard**

Monitoring cloud usage is essential for all customers. Understanding cloud spend is the first step toward cost optimization. Many organizations need comprehensive tools to monitor their cloud usage effectively. In this lab, we'll show you how to build a solution to view your Oracle Cloud Infrastructure (OCI) usage data using OCI's native analytics capabilities. You'll create a data pipeline that securely manages your OCI usage data and build dashboards that automatically refresh daily to display cost trends. While familiarity with the OCI Console is helpful, we'll guide you through the design and setup process step by step.

This workshop is designed for FinOps teams, Solution Architects, Oracle Partners, and anyone interested in monitoring and analyzing their OCI usage.

![Cloud Intelligence Dashboard for Oracle Cloud Infrastructure (OCI) Architecture](/images/cid-oci-highlevel-deployment.png)

---

## Goals

* Deploy a data pipeline to collect OCI cost management data (CUR) into Amazon Simple Storage Service (Amazon S3) on a daily schedule.
* Perform an initial manual pull of data.
* Deploy a sample QuickSight dashboard or use existing AWS FOCUS dashboard
* Apply a basic set of dashboard customisations.
* Learn how to manage the solution during operation.

---

## AWS Services being used
![AWS Services used in solution](/images/cid-oci-aws-services.png)

---

## Prerequisites

**AWS**
1. An existing **AWS Account** or you must complete the [AWS account setup lab.](https://wellarchitectedlabs.com/cost/100_labs/100_1_aws_account_setup/)
2. Ability to run CloudFormation template to deploy the CID OCI FOCUS stack
3. An Amazon **QuickSight Enterprise Edition Account**. If you do not already have one follow instructions [here.](https://aws.amazon.com/premiumsupport/knowledge-center/quicksight-enterprise-account/)
4. Enough **QuickSight SPICE capacity** to accommodate your data set. To purchase SPICE, follow instructions [here.](https://docs.aws.amazon.com/quicksight/latest/user/managing-spice-capacity.html#spice-capacity-purchasing)



**Oracle Cloud Ifrastructure**
1. **IAM Policy** - To allow access to CUR objects in OCI Tenancy. Keep a Note of the Tenancy OCID for later use.
In your Tenancy you will need to define a Policy Statement for Cost and Usage reporting. Below is the current recommended policy. Change only Identity Group. Follow OCI guide [here.](https://docs.oracle.com/en-us/iaas/Content/Billing/Concepts/costusagereportsoverview.htm#policy)

2. **Cost and Usage Reports** - Cost and usage reports are comma-separated value (CSV) files that are generated daily and stored in an Object Storage bucket. By default this will be a Oracle owned Object Storage bucket, but you can also download to another Object storage bucket owned by you. Follow OCI guide here (https://www.ateam-oracle.com/post/automating-the-export-of-oci-finops-open-cost-and-usage-specification-focus-reports-to-object-storage). 

3. **Customer Secret Keys** - To allow API based access to your OCI resources. You can follow the OCI guide [here.](https://docs.oracle.com/en-us/iaas/Content/Identity/Tasks/managingcredentials.htm#)

3. **Authentication and Authorization** - An administrator in your organization needs to set up groups, compartments, and policies  that control which users can access which services, which resources, and the type of access. Follow the OCI guide [here.](https://docs.oracle.com/en-us/iaas/Content/Billing/Concepts/costusagereportsoverview.htm#auth)

4. **Tenancy Information** - If using the Oracle owned Objectstorage for Cost and Usage Report use namespace called **bling**. If you are using a different Objectstorage bucket, use the appropriate namespace. Follow the OCI guide [here.](https://docs.oracle.com/en-us/iaas/compute-cloud-at-customer/topics/object/obtaining-the-object-storage-namespace.htm). Also collect the **home region** of tenancy to know which Objectstorage endpoint to use. 

Once you've setup OCI, you'll have the following information and are ready to start the workshop:

- [x] ObjecStorage Endpoint URL: https://**[namespace]**.compat.objectstorage.**[region]**.oraclecloud.com 
- [x] OCI Tenancy Unique ID (OCID)
- [x] OCI Secret Access Key
- [x] OCI Secret Access Secret


## References:

* https://blogs.oracle.com/cloud-infrastructure/post/announcing-focus-support-for-oci-cost-reports
* https://www.ateam-oracle.com/post/automating-the-export-of-oci-finops-open-cost-and-usage-specification-focus-reports-to-object-storage


## Contributing

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

---

## License

This library is licensed under the MIT-0 License. See the [LICENSE](/LICENSE) file.

---