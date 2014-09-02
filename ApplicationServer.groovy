/**
 * Created by whoisjeremylam on 12/05/14.
 *
 * Thanks to https://bitbucket.org/jsumners/restlet-2.1-demo for a sane example using Restlet
 */
import groovy.json.JsonSlurper
import org.restlet.Component
import org.restlet.data.Protocol
import org.restlet.data.Status
import org.restlet.resource.Post
import org.restlet.resource.ServerResource
import org.restlet.Application
import org.restlet.Restlet
import org.restlet.routing.Router
import org.json.JSONException
import org.json.JSONObject
import java.net.URLEncoder

@Grab(group='log4j', module='log4j', version='1.2.17')
@Grab(group='org.xerial', module='sqlite-jdbc', version='3.7.2')

@Grab(group='org.restlet.jse', module='org.restlet', version = '2.2.0')
@Grab(group='org.restlet.jse', module='org.restlet.ext.json', version = '2.2.0')
@Grab(group='org.restlet.jse', module='org.restlet.lib.org.json', version = '2.0') //org.restlet.jse:org.restlet.lib.org.json:2.0

import org.apache.log4j.*
import groovy.sql.Sql


class ApplicationServer {

	 

    public static class ApplicationServerResource extends ServerResource {
		
		def db
		def databaseName
		def myLogger
		def log4j
		
        @Override
        public void doInit() {
			// Set up some log4j stuff
			myLogger = new Logger()
			PropertyConfigurator.configure("ApplicationServer_log4j.properties")
			log4j = myLogger.getRootLogger()
			log4j.setLevel(Level.INFO)

	        def iniConfig = new ConfigSlurper().parse(new File("ApplicationServer.ini").toURL())
			databaseName = iniConfig.database.name
			db = Sql.newInstance("jdbc:sqlite:${databaseName}", "org.sqlite.JDBC")
			ApplicationDBCreator.createDB(db)
	    }
		
		@Post
        public String post(value) {           		
			log4j.info("insert into codes values ${code}, 'init', 0")
            db.execute("insert into codes values ${code}, 'init', 0")
            response = this.getResponse()
            response.setStatus(Status.SUCCESS_CREATED)
            response.setEntity()
        }
		
    }


    public static class ApplicationServerApplication extends Application {

        /**
         * Creates a root Restlet that will receive all incoming calls.
         */
        @Override
        public synchronized Restlet createInboundRoot() {
            // Create a router Restlet that routes each call to a new instance of HelloWorldResource.
            Router router = new Router(getContext())

            router.attach("/data", ApplicationServerResource.class)

            return router
        }

    }

    static init() {

    }


    public static void main(String[] args) throws Exception {
        def serverApp = new ApplicationServerApplication()
        init()

        // Create a new Component.
        Component component = new Component()
       
        component.getServers().add(Protocol.HTTP, 8080)
				
        // Attach the sample application.
        component.getDefaultHost().attach("/lazooz", serverApp)

        // Start the component.
        component.start()
    }
}
