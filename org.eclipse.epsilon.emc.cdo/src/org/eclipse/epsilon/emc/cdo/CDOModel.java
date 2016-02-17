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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.emf.cdo.CDOObject;
import org.eclipse.emf.cdo.CDOObjectReference;
import org.eclipse.emf.cdo.common.protocol.CDOProtocolConstants;
import org.eclipse.emf.cdo.common.revision.CDORevision;
import org.eclipse.emf.cdo.eresource.CDOResource;
import org.eclipse.emf.cdo.net4j.CDONet4jSession;
import org.eclipse.emf.cdo.net4j.CDONet4jSessionConfiguration;
import org.eclipse.emf.cdo.net4j.CDONet4jUtil;
import org.eclipse.emf.cdo.transaction.CDOTransaction;
import org.eclipse.emf.cdo.util.CDOUtil;
import org.eclipse.emf.cdo.util.CommitException;
import org.eclipse.emf.cdo.view.CDOQuery;
import org.eclipse.emf.cdo.view.CDOView;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.epsilon.common.util.StringProperties;
import org.eclipse.epsilon.emc.emf.AbstractEmfModel;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
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
	public static final String PROPERTY_CDO_FEATANALYZER = "cdo.featureAnalyzer";

	private String cdoName, cdoURL, cdoPath;
	private CDOTransaction cdoTransaction;
	private int cdoCollectionInitial = 0, cdoCollectionRChunk = 300, cdoRevPrefetching = 100;
	private boolean useFeatureAnalyzer = false;

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
		this.useFeatureAnalyzer = properties.hasProperty(PROPERTY_CDO_FEATANALYZER);

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
			if (useFeatureAnalyzer) {
				sessionConfig.setFetchRuleManager(CDOUtil.createThreadLocalFetchRuleManager());
			}

			// This would need a custom server - default one cannot be configured
			// to use gzip compression, as far as I can see.
			//sessionConfig.setStreamWrapper(new GZIPStreamWrapper());
			final CDONet4jSession cdoSession = sessionConfig.openNet4jSession();
			
			// Some tweaks were taken from https://wiki.eclipse.org/CDO/Tweaking_Performance
			cdoSession.options().setCollectionLoadingPolicy(CDOUtil.createCollectionLoadingPolicy(cdoCollectionInitial, cdoCollectionRChunk));
			cdoTransaction = cdoSession.openTransaction();
			cdoTransaction.options().setRevisionPrefetchingPolicy(CDOUtil.createRevisionPrefetchingPolicy(cdoRevPrefetching));
			if (useFeatureAnalyzer) {
				cdoTransaction.options().setFeatureAnalyzer(CDOUtil.createModelBasedFeatureAnalyzer());
			}

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
				/*
				 * Note: rolling back produces errors sometimes: if we don't
				 * store on disposal, we simply close the transaction without
				 * committing any changes.
				 */
				if (isStoredOnDisposal()) {
					cdoTransaction.commit();
				}
			} catch (CommitException e) {
				e.printStackTrace();
			}
			cdoTransaction.close();
			cdoTransaction.getSession().close();
		}
	}

	@Override
	public EClass classForName(String name) throws EolModelElementTypeNotFoundException {
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
	protected boolean deleteElementInModel(Object instance) throws EolRuntimeException {
		// See https://www.eclipse.org/forums/index.php/t/156816/
		// ("Delete elements in the model very slowly").

		// Fetch the entire subtree to be removed
		final CDOObject eob = (CDOObject) instance;
		final Set<CDOObject> toRemove = new HashSet<>();
		for (TreeIterator<EObject> it = eob.eAllContents(); it.hasNext(); ) {
			toRemove.add((CDOObject) it.next());
		}

		// Disconnect the entire subtree
		List<CDOObjectReference> refs = cdoTransaction.queryXRefs(toRemove);
		for (CDOObjectReference ref : refs) {
			final CDOObject src = ref.getSourceObject();
			final CDOObject target = ref.getTargetObject();
			final EStructuralFeature feature = ref.getSourceFeature();
			if (feature.isDerived() || !feature.isChangeable()) {
				continue;
			}

			EcoreUtil.remove(src, feature, target);
		}

		for (CDOObject cdoObject : toRemove) {
			EcoreUtil.remove(cdoObject);
		}

		return true;
	}

	@Override
	protected Collection<EObject> getAllOfTypeFromModel(String type) throws EolModelElementTypeNotFoundException {
		final EClass eClass = classForName(type);

		final CDOView cdoView = getCDOResource().cdoView();
		final CDOQuery query = cdoView.createQuery(CDOProtocolConstants.QUERY_LANGUAGE_INSTANCES, null);
		query.getParameters().put(CDOProtocolConstants.QUERY_LANGUAGE_INSTANCES_TYPE, eClass);
		query.getParameters().put(CDOProtocolConstants.QUERY_LANGUAGE_INSTANCES_EXACT, true);
		final List<EObject> allInstances = query.getResult();

		return filterByResource(allInstances);
	}

	@Override
	protected Collection<EObject> getAllOfKindFromModel(String kind) throws EolModelElementTypeNotFoundException {
		final EClass eClass = classForName(kind);
		final List<EObject> allInstances = getCDOResource().cdoView().queryInstances(eClass);
		return filterByResource(allInstances);
	}

	protected Collection<EObject> filterByResource(final List<EObject> allInstances) {
		final List<EObject> filtered = new ArrayList<>();
		for (EObject eob : allInstances) {
			if (eob.eResource() == modelImpl) {
				filtered.add(eob);
			}
		}
		return filtered;
	}

	@Override
	protected Collection<EObject> allContentsFromModel() {
		getCDOResource().cdoPrefetch(CDORevision.DEPTH_INFINITE);
		return super.allContentsFromModel();
	}

	protected CDOResource getCDOResource() {
		return (CDOResource)modelImpl;
	}
}
