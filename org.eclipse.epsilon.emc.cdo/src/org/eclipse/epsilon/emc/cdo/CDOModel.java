/*******************************************************************************
 * Copyright (c) 2016 University of York.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *    Antonio Garcia-Dominguez - initial API and implementation
 *******************************************************************************/
package org.eclipse.epsilon.emc.cdo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.eclipse.emf.cdo.common.revision.CDORevision;
import org.eclipse.emf.cdo.eresource.CDOResource;
import org.eclipse.emf.cdo.net4j.CDONet4jSession;
import org.eclipse.emf.cdo.net4j.CDONet4jSessionConfiguration;
import org.eclipse.emf.cdo.net4j.CDONet4jUtil;
import org.eclipse.emf.cdo.transaction.CDOTransaction;
import org.eclipse.emf.cdo.util.CDOUtil;
import org.eclipse.emf.cdo.util.CommitException;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.epsilon.common.util.StringProperties;
import org.eclipse.epsilon.emc.emf.AbstractEmfModel;
import org.eclipse.epsilon.eol.exceptions.models.EolModelElementTypeNotFoundException;
import org.eclipse.epsilon.eol.exceptions.models.EolModelLoadingException;
import org.eclipse.epsilon.eol.models.IRelativePathResolver;
import org.eclipse.net4j.Net4jUtil;
import org.eclipse.net4j.connector.IConnector;
import org.eclipse.net4j.util.container.IPluginContainer;

/**
 * CDO driver for Epsilon.
 */
public class CDOModel extends AbstractEmfModel {

	public static final String PROPERTY_CDO_URL = "cdo.url";
	public static final String PROPERTY_CDO_NAME = "cdo.repo";
	public static final String PROPERTY_CDO_PATH = "cdo.path";
	public static final String PROPERTY_CDO_COLLECTION_INITIAL = "cdo.collection.initial";
	public static final String PROPERTY_CDO_COLLECTION_RCHUNK = "cdo.collection.rchunk";
	public static final String PROPERTY_CDO_REVPREFETCH = "cdo.revprefetch";

	private String cdoName, cdoURL, cdoPath;
	private CDOTransaction cdoTransaction;
	private int cdoCollectionInitial = 0, cdoCollectionRChunk = 300, cdoRevPrefetching = 100;

	@Override
	public void load(StringProperties properties, IRelativePathResolver resolver) throws EolModelLoadingException {
		super.load(properties, resolver);
		this.cdoURL = (String) properties.get(PROPERTY_CDO_URL);
		this.cdoName = (String) properties.get(PROPERTY_CDO_NAME);
		this.cdoPath = (String) properties.get(PROPERTY_CDO_PATH);

		if (properties.hasProperty(PROPERTY_CDO_COLLECTION_INITIAL)) {
			this.cdoCollectionInitial = Integer.valueOf(properties.get(PROPERTY_CDO_COLLECTION_INITIAL).toString());
		}
		if (properties.hasProperty(PROPERTY_CDO_COLLECTION_RCHUNK)) {
			this.cdoCollectionRChunk = Integer.valueOf(properties.get(PROPERTY_CDO_COLLECTION_RCHUNK).toString());
		}
		if (properties.hasProperty(PROPERTY_CDO_REVPREFETCH)) {
			this.cdoRevPrefetching = Integer.valueOf(properties.get(PROPERTY_CDO_REVPREFETCH).toString());
		}

		load();
	}

	@Override
	protected void loadModel() throws EolModelLoadingException {
		try {
			final IConnector connector = Net4jUtil.getConnector(IPluginContainer.INSTANCE, cdoURL);
			final CDONet4jSessionConfiguration sessionConfig = CDONet4jUtil.createNet4jSessionConfiguration();
			sessionConfig.setConnector(connector);
			sessionConfig.setRepositoryName(cdoName);
			// tests with singleton query don't reveal benefits with CDO feature analyzers
			//sessionConfig.setFetchRuleManager(CDOUtil.createThreadLocalFetchRuleManager());

			// This would need a custom server - default one cannot be configured
			// to use gzip compression, as far as I can see.
			//sessionConfig.setStreamWrapper(new GZIPStreamWrapper());
			final CDONet4jSession cdoSession = sessionConfig.openNet4jSession();
			
			// Some tweaks were taken from https://wiki.eclipse.org/CDO/Tweaking_Performance
			cdoSession.options().setCollectionLoadingPolicy(CDOUtil.createCollectionLoadingPolicy(cdoCollectionInitial, cdoCollectionRChunk));
			cdoTransaction = cdoSession.openTransaction();
			cdoTransaction.options().setRevisionPrefetchingPolicy(CDOUtil.createRevisionPrefetchingPolicy(cdoRevPrefetching));
			//cdoTransaction.options().setFeatureAnalyzer(CDOUtil.createModelBasedFeatureAnalyzer());
			modelImpl = cdoTransaction.getResource(cdoPath);
			registry = cdoTransaction.getSession().getPackageRegistry();
		} catch (Exception ex) {
			throw new EolModelLoadingException(ex, this);
		}
	}

	@Override
	public void disposeModel() {
		super.disposeModel();

		if (cdoTransaction != null) {
			try {
				cdoTransaction.commit();
			} catch (CommitException e) {
				e.printStackTrace();
			}
			cdoTransaction.close();
			cdoTransaction.getSession().close();
		}
	}

	@Override
	public EClass classForName(String name) throws EolModelElementTypeNotFoundException {
		// TODO Auto-generated method stub
		boolean absolute = name.indexOf("::") > -1;

		for (Object pkg : registry.values()) {
			if (pkg instanceof EPackage.Descriptor) {
				pkg = ((EPackage.Descriptor)pkg).getEPackage();
			}
			if (pkg instanceof EPackage) {
				EClass eClass = classForName(name, absolute, pkg);
				if (eClass != null) {
					return eClass;
				}
			}
		}
		return null;
	}

	@Override
	protected Collection<EObject> getAllOfTypeFromModel(String type) throws EolModelElementTypeNotFoundException {
		final EClass eClass = classForName(type);

		final List<EObject> filtered = new ArrayList<>();
		for (EObject eob : cdoTransaction.queryInstances(eClass)) {
			if (eob.eClass() == eClass) {
				filtered.add(eob);
			}
		}

		return filtered;
	}

	@Override
	protected Collection<EObject> getAllOfKindFromModel(String kind) throws EolModelElementTypeNotFoundException {
		final EClass eClass = classForName(kind);
		return cdoTransaction.queryInstances(eClass);
	}

	@Override
	protected Collection<EObject> allContentsFromModel() {
		((CDOResource)modelImpl).cdoPrefetch(CDORevision.DEPTH_INFINITE);
		return super.allContentsFromModel();
	}
}
