EMC driver for CDO models
===

The plugins developed in this repository extend the [Eclipse Epsilon](http://www.eclipse.org/epsilon/) model management framework to allow it to read and write models stored in an [Eclipse CDO](https://eclipse.org/cdo/) repository.

To use this driver, install Epsilon first, then install all the features from this update site and let Eclipse restart:

```
http://epsilonlabs.github.io/emc-cdo/updates/
```

You should be able to use a new "CDO Model" model type within your E*L launch configurations. You may need to check the "Show all model types" checkbox in order to see it, though. The model type will ask for the connection URL to the CDO repository, which is the same one used in the "CDO Repositories" view.

This resource takes advantage of remote CDO queries to speed up `X.allInstances`, prioritizes the remote EPackage registry over the local global registry, and speeds up deletion by using the CDO queryXRefs method (instead of the naive iteration through the whole resource that ECoreUtil#delete does).
