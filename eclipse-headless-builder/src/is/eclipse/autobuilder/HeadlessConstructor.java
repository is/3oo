package is.eclipse.autobuilder;

import org.eclipse.core.resources.IMarker;
import org.eclipse.core.resources.IProject;
import org.eclipse.core.resources.IProjectDescription;
import org.eclipse.core.resources.IResource;
import org.eclipse.core.resources.IWorkspace;
import org.eclipse.core.resources.IWorkspaceDescription;
import org.eclipse.core.resources.IWorkspaceRoot;
import org.eclipse.core.resources.IncrementalProjectBuilder;
import org.eclipse.core.resources.ResourcesPlugin;
import org.eclipse.core.runtime.IPath;
import org.eclipse.equinox.app.IApplication;
import org.eclipse.equinox.app.IApplicationContext;

public class HeadlessConstructor implements IApplication {
  final String version = "0.0.0.3";
  boolean showWarning = false;
  
	@Override
	public Object start(IApplicationContext appCtx) throws Exception {
	  String arguments[] = (String[])appCtx.getArguments().get("application.args");
	  String projectNames[] = null;
	  
	  for (String arg : arguments) {
	    if (arg.startsWith("-projects=")) {
	      projectNames = arg.substring("-projects=".length()).split(",");
	    }
	    if (arg.startsWith("-showwarning")) {
	      showWarning = true;
	    }
	  }
	  
	  
	  if (projectNames == null) {
	    return EXIT_OK;
	  }
	  
	  System.out.format("Auto Headless Constructor Plugin: %s\n", version);
		IWorkspace ws = ResourcesPlugin.getWorkspace();
		
		// Disable auto building.
		IWorkspaceDescription wsDesc = ws.getDescription();
		wsDesc.setAutoBuilding(false);
		
		IWorkspaceRoot wsRoot = ws.getRoot();
		IPath wsPath = wsRoot.getLocation();
		System.out.format("Workspace location is %s\n", wsPath.toString());
		
		for (String projectName : projectNames) {
		  IProject proj = wsRoot.getProject(projectName);
		  if (!proj.exists()) {
		    IProjectDescription projDesc = ws.loadProjectDescription(wsPath.append(projectName).append(".project"));
		    proj.create(projDesc, null);
		    System.out.format("sub project: %s added\n", projectName);
		  } else {
		    System.out.format("sub project: %s exists\n",projectName);
		  }
		  if (!proj.isOpen()) {
		    proj.open(null);
		  }
		}
		
		
		System.out.format("There is %d projects\n", wsRoot.getProjects().length);
		
		// Begin auto build
		System.out.println("Auto build workspace");
		ws.build(IncrementalProjectBuilder.CLEAN_BUILD, null);
		
		IWorkspace.ProjectOrder projOrder = ws.computeProjectOrder(wsRoot.getProjects());		
		for (IProject project : projOrder.projects) {
		  System.out.println("building " + project.getName() + " ...");
		  project.build(IncrementalProjectBuilder.FULL_BUILD, null);
		}

		IMarker[] markers = wsRoot.findMarkers(IMarker.PROBLEM, true, IResource.DEPTH_INFINITE);
		if (markers == null)
		{
		  return EXIT_OK;
		}
		
		int errorNum = 0;
		int warnNum = 0;
		for (IMarker marker: markers) {
		  int serverity = (Integer)marker.getAttribute(IMarker.SEVERITY);
		  String serverityStr = null;
		
		  if (serverity == IMarker.SEVERITY_ERROR) {
		    errorNum += 1;
		    serverityStr = "ERROR";
	      
		    System.out.format("%s:%d %s - %s\n",
	          marker.getResource().getFullPath().toString().substring(1),
	          marker.getAttribute(IMarker.LINE_NUMBER),
	          serverityStr,
	          marker.getAttribute(IMarker.MESSAGE));
		  } else if(serverity == IMarker.SEVERITY_WARNING) {
		    warnNum += 1;
		    serverityStr = "WARN";
		  
		    if (showWarning) {
	        System.out.format("%s:%d %s - %s\n",
	            marker.getResource().getFullPath().toString().substring(1),
	            marker.getAttribute(IMarker.LINE_NUMBER),
	            serverityStr,
	            marker.getAttribute(IMarker.MESSAGE));		      
		    }
		  } else if(serverity == IMarker.SEVERITY_INFO){
		    serverityStr = "INFO";
		  } else {
		    serverityStr = "UNSET";
		  }		  
		}
		
		System.out.format("%d errors, %d warnings\n", errorNum, warnNum);
		
		if (errorNum == 0)
		{
		  return EXIT_OK;
		}
		
		return new Integer(1);
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
	}
}
