# How To Contribute

There is a rapidly growing list of projects and companies relying on Cascading. Keeping Cascading exceptionally 
dependable and stable is paramount.    

But for Cascading to continue to evolve and meet expectations of its users, enhancements and fixes from third-parties 
are essential. Oftentimes meeting these goals is not frictionless.

To mitigate this, we have set up guidelines and expectations for contributions, along with added clarity of our 
commitment.

At the end of the day, the Cascading team is responsible for the quality of Cascading core, this entails 
supporting all code in the core, which is a large commitment. 

To keep this manageable, we prefer to open up new APIs for extension over adding enhancements directly to Cascading. As
Cascading is a framework, this philosophy fits nicely compared to other projects. 

In lieu of that, for any major or deep enhancements we expect to guide the design from inception in order to retain 
conceptual integrity throughout Cascading and across each release. 

This will minimize our long term burden, and time spent over the back and forth discussions over any premature patch. 
It will also ensure future compatibility with upcoming versions and proposed changes. 

So initiating a discussion on the mail list before development, and continuing against a well reasoned and tested GitHub 
pull request are reasonable expectations.

## Bugs and Minor Changes

If simply reporting a bug, look for a comparable test, reproduce your issue as a failing test added to one of the 
existing suites.

If suggesting a trivial change (fixing a typo, etc), please simply bring attention via the mail list. We are happy to
make the changes.

### Getting Started

* Make sure you have joined the [Cascading User mailing list](http://cascading.org/support/)
* Make sure you have a [GitHub account](https://github.com/signup/free)
* **Email the list with your feature or bug fix proposal**
  * Clearly describe the issue
  * Make sure you note the earliest version that you know has the issue
* Fork the repository on GitHub from [Cascading/cascading-local](https://github.com/Cascading/cascading-local)
  * If resolving an issue with a current pre-release WIP, fork from [cwensel/cascading](https://github.com/cwensel/cascading-local)
* Create a topic branch from where you want to base your work
  * This is usually the release branch (2.5) or release tag (2.5.5). Or wip releases (wip-2.6) 
  * Only target release branches if you are certain your fix must be on that branch
  * To quickly create a topic branch based on a release; `git checkout -b wip-2.5-topic origin/2.5`, 
    where 'topic' is describes your change
* Make commits of logical units, the fewer the better
  * A feature or fix per commit is reasonable, if not preferred

### Testing Changes

Cascading requires Gradle to build, see the README for the latest supported version.

Calling:

    > gradle clean check 

will run all tests for the Cascading local mode platform. This works for other supported platforms.

### Submitting Changes

* Sign the [Wensel Contributor License Agreement](http://files.wensel.net/agreements/Wensel_Contributor_Agreement.doc)
  and email as a PDF to chris@wensel.net
* Push your changes to a topic branch in your fork of the repository
* Submit a pull request to the repository in the Cascading organization
* Follow up on the original email thread with a link to the pull request
* After feedback has been given we expect responses within two weeks, after which we may close the pull request 
  if it isn't showing any activity

### Next Steps

The Cascading team will review any requests and provide feedback. Hopefully this will be minimal if we had a prior
discussion regarding the enhancements. 

Regardless of the process, we reserve the right to decide when and if an enhancement will be included in any given 
release. We prefer to push large changes into major or minor releases, and small changes in minor or maintenance 
releases. This decision is based on the level and quality of changes that need to be acknowledged and absorbed, if at 
all, by the end-users.

We will not merge a pull request directly from GitHub. We find the resulting git history complicated and unnecessary. 

Subsequently most commits are rebased and cleaned up on our side, and all are committed with full attribution of the 
original author.