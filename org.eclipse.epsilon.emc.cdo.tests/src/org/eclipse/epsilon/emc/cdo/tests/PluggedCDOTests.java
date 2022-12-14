package org.eclipse.epsilon.emc.cdo.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import org.eclipse.emf.cdo.internal.server.mem.MEMStore;
import org.eclipse.emf.cdo.server.CDOServerUtil;
import org.eclipse.emf.cdo.server.IRepository;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.epsilon.emc.cdo.CDOModel;
import org.eclipse.epsilon.emc.emf.EmfUtil;
import org.eclipse.epsilon.eol.exceptions.models.EolModelLoadingException;
import org.eclipse.net4j.jvm.IJVMAcceptor;
import org.eclipse.net4j.jvm.JVMUtil;
import org.eclipse.net4j.util.container.IPluginContainer;
import org.eclipse.net4j.util.lifecycle.LifecycleUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("restriction")
public class PluggedCDOTests {

	private IRepository repo;
	private IJVMAcceptor acceptor;

	@Before
	public void setup() {
		repo = CDOServerUtil.createRepository("repo", new MEMStore(), Collections.emptyMap());
		CDOServerUtil.addRepository(IPluginContainer.INSTANCE, repo);
		acceptor = JVMUtil.getAcceptor(IPluginContainer.INSTANCE, "local");
	}

	private CDOModel createModel(String path) {
		CDOModel model = new CDOModel();
		model.setServerURL("jvm://local");
		model.setRepositoryName("repo");
		model.setModelPath(path);
		return model;
	}

	@After
	public void teardown() {
		if (acceptor != null) {
			LifecycleUtil.deactivate(acceptor);
		}
		LifecycleUtil.deactivate(repo);
	}

	@Test
	public void missingModelNoCreate() {
		CDOModel model = createModel("/missing-model");
		model.setCreateMissingResource(false);
		assertThrows(
			"Trying to load a missing model without creating it should fail",
			EolModelLoadingException.class, () -> model.load());
	}

	@Test
	public void missingModelCreateEmpty() throws Exception {
		CDOModel model = createModel("/missing-model");
		model.setCreateMissingResource(true);
		model.load();
		assertEquals("Loading a newly created resource should result in an empty model", 0, model.allContents().size());
		model.dispose();
	}

	@Test
	public void createTree() throws Exception {
		registerTreeMetamodel();

		CDOModel model = createModel("/tree");
		model.setStoredOnDisposal(true);
		model.setCreateMissingResource(true);
		model.load();

		EObject tree = model.createInstance("Tree");
		assertNotNull("Tree has been created", tree);
		assertEquals("There should be one Tree in the newly created resource", 1, model.getAllOfKind("Tree").size());
		model.dispose();

		CDOModel model2 = createModel("/tree");
		model2.setCreateMissingResource(false);
		model2.setStoredOnDisposal(false);
		model2.load();
		assertEquals("There should be one Tree in the reloaded resource", 1, model2.getAllOfKind("Tree").size());
		model2.dispose();
	}

	private void registerTreeMetamodel() throws IOException {
		Resource r = EmfUtil.createResource(URI.createFileURI(new File("metamodels/Tree.ecore").getCanonicalPath()));
		r.load(null);
		EPackage pkg = (EPackage) r.getContents().get(0);
		EPackage.Registry.INSTANCE.put(pkg.getNsURI(), pkg);
	}
}
