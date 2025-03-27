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

AWS

* An existing AWS Account or you must complete the AWS account setup lab.
* An Amazon QuickSight Enterprise Edition Account. If you do not already have one follow instructions here.
* Enough QuickSight SPICE capacity to accommodate your data set. To purchase SPICE, follow instructions here.
* Complete the FOCUS setup - here. 
    * The FOCUS framework will provide many of the necessary setup and resources which OCI FOCUS dashboard with increment.
* Ability to run CloudFormation Template to deploy the CID OCI FOCUS stack
* S3 Bucket for uploading some dependant artefacts


OCI

* Required IAM Policy - In your Tenancy you will need to define a Policy Statement for Cost and Usage reporting. Below is the current recommended policy. Change only Identity Group. Follow OCI guide here

define tenancy usage-report as ocid1.tenancy.oc1..aaaaaaaaned4fkpkisbwjlr56u7cj63lf3wffbilvqknstgtvzub7vhqkggq
endorse group <group> to read objects in tenancy usage-report

* This example has a specific tenancy OCID, because the reports are stored in an Oracle-owned Object Storage bucket hosted by Oracle Cloud Infrastructure, and not a customer's tenancy.
* Customer Secret - You can follow the OCI guide here. Ensure to have the user added in the Group mentioned in the policy
* Check the FOCUS Reports are accessible.
    * Click Billing and Cost Management. Under Cost Management, click Cost and Usage Reports. 
    * Follow the guide here
*  


Once you've setup OCI, you'll have the following information and are ready to start the workshop:

    * OCI ObjecStorage Endpoint URL: https://<namespace>.compat.objectstorage.<region>.oraclecloud.com
    * OCI Tenancy Unique ID (OCID)
    * OCI Secret Access Key
    * OCI Secret Access Secret
* 


## References:

* https://blogs.oracle.com/cloud-infrastructure/post/announcing-focus-support-for-oci-cost-reports
* https://www.ateam-oracle.com/post/automating-the-export-of-oci-finops-open-cost-and-usage-specification-focus-reports-to-object-storage


## Contributing

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

---

## License

This library is licensed under the MIT-0 License. See the [LICENSE](/LICENSE) file.

---