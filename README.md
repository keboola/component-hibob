HiBob Extractor
=============

HiBob Extractor allows you to extract data from HR platform HiBob.
To use this extractor, you will need a Service User and Service User Token.

**Table of contents:**

[TOC]

Prerequisites
=============

- Service User ID
- Service User Token

You can find out how to set up the service user in [HiBoB Documentation](https://apidocs.hibob.com/docs/api-service-users).
You can also use TEST CONNECTION button to validate your credentials.


Supported endpoints
===================

- [Employees](https://apidocs.hibob.com/reference/post_people-search) (this is the default endpoint that gets downloaded each run)
- [Employment History](https://apidocs.hibob.com/reference/get_people-id-employment)
- [Employee Lifecycle](https://apidocs.hibob.com/reference/get_people-id-lifecycle)
- [Employee Work History](https://apidocs.hibob.com/reference/get_people-id-work)

If you need more endpoints, please submit your request to
[ideas.keboola.com](https://ideas.keboola.com/)

Configuration
=============

Aside from endpoints, you can select the `Human readable` parameter which makes the API return object's names instead of IDs.

Output
======

If all endpoints are selected, the component outputs four tables: `employees`, `employee_work_history`, `employment_history`, `employee_lifecycle`.

Development
-----------

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to
your custom path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, init the workspace and run the component with following
command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone https://bitbucket.org/kds_consulting_team/kds-team.ex-hibob kds-team.ex-hibob
cd kds-team.ex-hibob
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and lint check using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For information about deployment and integration with KBC, please refer to the
[deployment section of developers
documentation](https://developers.keboola.com/extend/component/deployment/)
