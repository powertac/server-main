/*
 * Copyright (c) 2011 by the original author
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.powertac.server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.powertac.common.Competition;
import org.powertac.common.PluginConfig;
import org.powertac.common.TimeService;
import org.powertac.common.XMLMessageConverter;
import org.powertac.common.interfaces.BootstrapDataCollector;
import org.powertac.common.interfaces.CompetitionSetup;
import org.powertac.common.interfaces.InitializationService;
import org.powertac.common.repo.DomainRepo;
import org.powertac.common.repo.PluginConfigRepo;
import org.powertac.common.spring.SpringApplicationContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * Manages command-line and file processing for pre-game simulation setup. 
 * A simulation can be started in one of two ways:
 * <ul>
 * <li>By running the process with command-line arguments as specified in
 * {@link PowerTacServer}, or</li>
 * <li> by calling the <code>preGame()</code> method to
 * set up the environment and allow configuration of the next game, through
 * a web (or REST) interface.</li>
 * </ul>
 * @author John Collins
 */
@Service
public class CompetitionSetupService
  implements CompetitionSetup//, ApplicationContextAware
{
  static private Logger log = Logger.getLogger(CompetitionSetupService.class);
  //static private ApplicationContext context;

  @Autowired
  private CompetitionControlService cc;

  @Autowired
  private BootstrapDataCollector defaultBroker;

  @Autowired
  private PluginConfigRepo pluginConfigRepo;

  @Autowired
  private ServerPropertiesService serverProps;

  @Autowired
  private XMLMessageConverter messageConverter;

  @Autowired
  private LogService logService;
  
  @Autowired
  private JmsManagementService jmsManagementService;

  private Competition competition;
  
  /**
   * Standard constructor
   */
  public CompetitionSetupService ()
  {
    super();
  }
  
  /**
   * Processes command-line arguments, which means looking for the specified 
   * script file and processing that
   */
  void processCmdLine (String[] args)
  {
    // pick up and process the command-line arg if it's there
    if (args.length > 1) {
      // cli setup
      processCli(args);
    }
    else if (args.length == 1) {
      // old-style script file
      processScript(args);
    }
    else { // args.length == 0
      // running from web interface
      System.out.println("Server BootStrap");
      //participantManagementService.initialize();
      preGame();

      // idle while the web interface controls the simulator
      while(true) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          // ignore this - it's how we exit
        }
      }
    }
  }
  
  // handles the server CLI as described at
  // https://github.com/powertac/powertac-server/wiki/Server-configuration
  private void processCli (String[] args)
  {
    // set up command-line options
    OptionParser parser = new OptionParser();
    OptionSpec<File> bootOutput = 
        parser.accepts("boot").withRequiredArg().ofType(File.class);
    OptionSpec<URL> controllerUrl =
        parser.accepts("control").withRequiredArg().ofType(URL.class);
    OptionSpec<URL> serverConfigUrl =
        parser.accepts("server-config").withRequiredArg().ofType(URL.class);
    OptionSpec<String> logSuffixOption =
        parser.accepts("log-suffix").withRequiredArg();
    parser.accepts("sim");
    OptionSpec<URL> bootData =
        parser.accepts("boot-data").withRequiredArg().ofType(URL.class);
    OptionSpec<String> brokerList =
        parser.accepts("brokers").withRequiredArg().withValuesSeparatedBy(',');
    
    // do the parse
    OptionSet options = parser.parse(args);
    
    try {
      // process common options
      String logSuffix = options.valueOf(logSuffixOption);
      URL controller = options.valueOf(controllerUrl);
      URL serverConfig = options.valueOf(serverConfigUrl);
      if (serverConfig == null && controller != null) {
        // offset from controller
        serverConfig = new URL(controller, "server-config");
      }
      // process serverConfig now, because other options may override
      // parts of it
      if (serverConfig != null)
        serverProps.setUserConfig(serverConfig);
      cc.init();      

      if (options.has(bootOutput)) {
        // bootstrap session
        setLogSuffix(logSuffix, controller, "boot");
        bootSession(options.valueOf(bootOutput));
      }
      else if (options.has("sim")) {
        // sim session
        setLogSuffix(logSuffix, controller, "sim");
        URL bootDataUrl = options.valueOf(bootData);
        if (bootDataUrl == null && controller != null)
          bootDataUrl = new URL(controller, "bootstrap-data");
        if (bootDataUrl != null)
          simSession(options.valuesOf(brokerList), bootDataUrl);
        else {
          System.err.println("Cannot run sim session without boot dataset");
          System.exit(1);
        }
      }
      else {
        // Must be either boot or sim
        System.err.println("Must provide either --boot or --sim to run server");
        System.exit(1);
      }
    }
    catch (OptionException e) {
      System.err.println("Bad command argument: " + e.toString());
    }
    catch (MalformedURLException e) {
      System.err.println("Cannot parse command line: " + e.toString());
      System.exit(1);
    }
    catch (IOException e) {
      System.err.println("I/O Error: " + e.toString());
      System.exit(1);
    }
  }

  // sets up the logfile name suffix
  private void setLogSuffix (String logSuffix,
                             URL controlUrl,
                             String defaultSuffix)
                                 throws IOException
  {
    if (logSuffix == null && controlUrl != null) {
      URL suffixUrl = new URL(controlUrl, "log-suffix");
      log.info("retrieving logSuffix from " + suffixUrl.toExternalForm());
      InputStream stream = suffixUrl.openStream();
      byte[] buffer = new byte[64];
      int len = stream.read(buffer);
      if (len > 0) {
        logSuffix = new String(buffer, 0, len);
        log.info("log suffix " + logSuffix + " retrieved");
      }
    }
    if (logSuffix == null)
      logSuffix = defaultSuffix;
    serverProps.setProperty("server.logfileSuffix", logSuffix);
    String realSuffix = serverProps.getProperty("server.logfileSuffix");
  }

  // handles the original script format. This capability can presumably
  // be jettisoned eventually.
  private void processScript (String[] args)
  {
    // running from config file
    System.out.println("old-style scriptfile interface");
    try {
      BufferedReader config = new BufferedReader(new FileReader(args[0]));
      String input;
      while ((input = config.readLine()) != null) {
        String[] tokens = input.split("\\s+");
        if ("bootstrap".equals(tokens[0])) {
          // bootstrap mode - optional config fn is tokens[2]
          if (tokens.length == 2 || tokens.length > 3) {
            System.out.println("Bad input " + input);
          }
          else {
            if (tokens.length == 3 && "--config".equals(tokens[1])) {
              // explicit config file - convert to URL format
              serverProps.setUserConfig(new URL("file:" + tokens[2]));
            }
            String bootstrapFilename =
                serverProps.getProperty("server.bootstrapDataFile",
                                        "/bd-noname.xml");
            cc.init();
            bootSession(new File(bootstrapFilename));
          }
        }
        else if ("sim".equals(tokens[0])) {
          int brokerIndex = 1;
          // sim mode, check for --config in tokens[1]
          if (tokens.length > 2 && "--config".equals(tokens[1])) {
            // explicit config file in tokens[2]
            serverProps.setUserConfig(new URL("file:" + tokens[2]));
            brokerIndex = 3;
          }
          log.info("In Simulation mode!!!");
          cc.init();
          String bootstrapFilename =
              serverProps.getProperty("server.bootstrapDataFile",
                                      "bd-noname.xml");
          File bootFile = new File(bootstrapFilename);
          if (!bootFile.canRead()) {
            System.out.println("Cannot read bootstrap data file " +
                               bootstrapFilename);
          }
          else {
            // collect broker names, hand to CC for login control
            ArrayList<String> brokerList = new ArrayList<String>();
            for (int i = brokerIndex; i < tokens.length; i++) {
              brokerList.add(tokens[i]);
            }
            URL bootDataset = new URL("file:" + bootFile);
            simSession(brokerList, bootDataset);
          }
        }
      }
    }
    catch (FileNotFoundException fnf) {
      System.out.println("Cannot find file " + args[0]);
    }
    catch (IOException ioe ) {
      System.out.println("Error reading file " + args[0]);
    }
  }

  // Runs a bootstrap session
  private void bootSession (File bootstrapFile) throws IOException
  {
    if (!bootstrapFile
          .getAbsoluteFile()
          .getParentFile()
          .canWrite()) {
      System.out.println("Cannot write to bootstrap data file " +
          bootstrapFile);                
    }
    else {
      FileWriter bootWriter = new FileWriter(bootstrapFile);
      cc.setAuthorizedBrokerList(new ArrayList<String>());
      preGame();
      //cc.runOnce(bootWriter);
      cc.runOnce(true);
      saveBootstrapData(bootWriter);
    }
  }

  // Runs a simulation session
  private void simSession (List<String> list, URL bootDataset)
  {
    cc.setAuthorizedBrokerList(list);
    if (preGame(bootDataset)) {
      cc.setBootstrapDataset(processBootDataset(bootDataset));
      cc.runOnce(false);
    }
  }  

  /**
   * Pre-game server setup - creates the basic configuration elements
   * to make them accessible to the web-based game-setup functions.
   * This method must be called when the server is started, and again at
   * the completion of each simulation. The actual simulation is started
   * with a call to competitionControlService.runOnce().
   */
  @Override
  public void preGame ()
  {    
    
    log.info("preGame() - start");
    // Create default competition
    competition = Competition.newInstance("defaultCompetition");
    //competitionId = competition.getId();
    String suffix = serverProps.getProperty("server.logfileSuffix", "x");
    logService.startLog(suffix);

    // Set up all the plugin configurations
    log.info("pre-game initialization");
    configureCompetition(competition);

    // Handle pre-game initializations by clearing out the repos,
    // then creating the PluginConfig instances
    List<DomainRepo> repos =
      SpringApplicationContext.listBeansOfType(DomainRepo.class);
    log.debug("found " + repos.size() + " repos");
    for (DomainRepo repo : repos) {
      repo.recycle();
    }
    List<InitializationService> initializers =
      SpringApplicationContext.listBeansOfType(InitializationService.class);
    log.debug("found " + initializers.size() + " initializers");
    for (InitializationService init : initializers) {
      init.setDefaults();
    }
    
    // start JMS here???
    jmsManagementService.start();
  }
  
  // configures a Competition from server.properties
  private void configureCompetition (Competition competition)
  {
    serverProps.configureMe(competition);
//    // get game length
//    int minimumTimeslotCount =
//        serverProps.getIntegerProperty("competition.minimumTimeslotCount",
//                                       competition.getMinimumTimeslotCount());
//    int expectedTimeslotCount =
//        serverProps.getIntegerProperty("competition.expectedTimeslotCount",
//                                       competition.getExpectedTimeslotCount());
//    if (expectedTimeslotCount < minimumTimeslotCount) {
//      log.warn("competition expectedTimeslotCount " + expectedTimeslotCount
//               + " < minimumTimeslotCount " + minimumTimeslotCount);
//      expectedTimeslotCount = minimumTimeslotCount;
//    }
//    int bootstrapTimeslotCount =
//        serverProps.getIntegerProperty("competition.bootstrapTimeslotCount",
//                                       competition.getBootstrapTimeslotCount());
//    int bootstrapDiscardedTimeslots =
//        serverProps.getIntegerProperty("competition.bootstrapDiscardedTimeslots",
//                                       competition.getBootstrapDiscardedTimeslots());
//
//    // get trading parameters
//    int timeslotsOpen =
//        serverProps.getIntegerProperty("competition.timeslotsOpen",
//                                       competition.getTimeslotsOpen());
//    int deactivateTimeslotsAhead =
//        serverProps.getIntegerProperty("competition.deactivateTimeslotsAhead",
//                                       competition.getDeactivateTimeslotsAhead());
//
//    // get time parameters
//    int timeslotLength =
//        serverProps.getIntegerProperty("competition.timeslotLength",
//                                       competition.getTimeslotLength());
//    int simulationTimeslotSeconds =
//        timeslotLength * 60 / (int)competition.getSimulationRate();
//    simulationTimeslotSeconds =
//        serverProps.getIntegerProperty("competition.simulationTimeslotSeconds",
//                                       simulationTimeslotSeconds);
//    int simulationRate = timeslotLength * 60 / simulationTimeslotSeconds;
//    DateTimeZone.setDefault(DateTimeZone.UTC);
//    DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd");
//    Instant start = null;
//    try {
//      start =
//        fmt.parseDateTime(serverProps.getProperty("competition.baseTime")).toInstant();
//    }
//    catch (Exception e) {
//      log.error("Exception reading base time: " + e.toString());
//    }
//    if (start == null)
//      start = competition.getSimulationBaseTime();
//
//    // populate the competition instance
//    competition
//      .withMinimumTimeslotCount(minimumTimeslotCount)
//      .withExpectedTimeslotCount(expectedTimeslotCount)
//      .withSimulationBaseTime(start)
//      .withSimulationRate(simulationRate)
//      .withTimeslotLength(timeslotLength)
//      .withSimulationModulo(timeslotLength * TimeService.MINUTE)
//      .withTimeslotsOpen(timeslotsOpen)
//      .withDeactivateTimeslotsAhead(deactivateTimeslotsAhead)
//      .withBootstrapTimeslotCount(bootstrapTimeslotCount)
//      .withBootstrapDiscardedTimeslots(bootstrapDiscardedTimeslots);
//    
//    // bootstrap timeslot timing is a local parameter
    int bootstrapTimeslotSeconds =
        serverProps.getIntegerProperty("server.bootstrapTimeslotSeconds",
                                       (int)(cc.getBootstrapTimeslotMillis()
                                             / TimeService.SECOND));
    cc.setBootstrapTimeslotMillis(bootstrapTimeslotSeconds * TimeService.SECOND);
  }

  /**
   * Sets up the simulator, with config overrides provided in a file
   * containing a sequence of PluginConfig instances. Errors are logged
   * if one or more PluginConfig instances cannot be used in the current
   * server setup.
   */
  public boolean preGame (URL bootFile)
  {
    log.info("preGame(File) - start");
    // run the basic pre-game setup
    preGame();
    
    // read the config info from the bootReader - 
    // We need to find a Competition and a set of PluginConfig instances
    Competition bootstrapCompetition = null;
    ArrayList<PluginConfig> configList = new ArrayList<PluginConfig>();
    //InputSource source = new InputSource(bootReader);
    XPathFactory factory = XPathFactory.newInstance();
    XPath xPath = factory.newXPath();
    try {
      // first grab the Competition
      XPathExpression exp =
          xPath.compile("/powertac-bootstrap-data/config/competition");
      NodeList nodes = (NodeList)exp.evaluate(new InputSource(bootFile.openStream()),
                                     XPathConstants.NODESET);
      String xml = nodeToString(nodes.item(0));
      bootstrapCompetition = (Competition)messageConverter.fromXML(xml);
      
      // then get the configs
      exp = xPath.compile("/powertac-bootstrap-data/config/plugin-config");
      nodes = (NodeList)exp.evaluate(new InputSource(bootFile.openStream()),
                                     XPathConstants.NODESET);
      // Each node is a plugin-config
      for (int i = 0; i < nodes.getLength(); i++) {
        Node node = nodes.item(i);
        xml = nodeToString(node);
        PluginConfig pic = (PluginConfig)messageConverter.fromXML(xml);
        configList.add(pic);
      }
    }
    catch (XPathExpressionException xee) {
      log.error("preGame: Error reading boot dataset: " + xee.toString());
      return false;
    }
    catch (IOException ioe) {
      log.error("preGame: Error opening file " + bootFile + ": " + ioe.toString());
    }
    // update the existing Competition - should be the current competition
    Competition.currentCompetition().update(bootstrapCompetition);
    
    // update the existing config, and make sure the bootReader has the
    // same set of PluginConfig instances as the running server
    for (Iterator<PluginConfig> pics = configList.iterator(); pics.hasNext(); ) {
      // find the matching one in the server and update it, then remove
      // the current element from the configList
      PluginConfig next = pics.next();
      PluginConfig match = pluginConfigRepo.findMatching(next);
      if (match == null) {
        // there's a pic in the file that's not in the server
        log.error("no matching PluginConfig found for " + next.toString());
        return false;
      }
      // if we found it, then we need to update it.
      match.update(next);
    }
    
    // we currently ignore cases where there's a config in the server that's
    // not in the file; there might be use cases for which this would
    // be useful.
    return true;
  }

  // method broken out to simplify testing
  void saveBootstrapData (Writer datasetWriter)
  {
    BufferedWriter output = new BufferedWriter(datasetWriter);
    List<Object> data = 
        defaultBroker.collectBootstrapData(competition.getBootstrapTimeslotCount());
    try {
      // write the config data
      output.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
      output.newLine();
      output.write("<powertac-bootstrap-data>");
      output.newLine();
      output.write("<config>");
      output.newLine();
      // current competition
      output.write(messageConverter.toXML(competition));
      output.newLine();
      // next the PluginConfig instances
      for (PluginConfig pic : pluginConfigRepo.list()) {
        output.write(messageConverter.toXML(pic));
        output.newLine();
      }
      output.write("</config>");
      output.newLine();
      // finally the bootstrap data
      output.write("<bootstrap>");
      output.newLine();
      for (Object item : data) {
        output.write(messageConverter.toXML(item));
        output.newLine();
      }
      output.write("</bootstrap>");
      output.newLine();
      output.write("</powertac-bootstrap-data>");
      output.newLine();
      output.close();
    }
    catch (IOException ioe) {
      log.error("Error writing bootstrap file: " + ioe.toString());
    }
  }

  // Extracts a bootstrap dataset from its file
  private ArrayList<Object> processBootDataset (URL bootDataset)
  {
    // Read and convert the bootstrap dataset
    ArrayList<Object> result = new ArrayList<Object>();
    XPathFactory factory = XPathFactory.newInstance();
    XPath xPath = factory.newXPath();
    try {
      InputSource source = new InputSource(bootDataset.openStream());
      // we want all the children of the bootstrap node
      XPathExpression exp =
          xPath.compile("/powertac-bootstrap-data/bootstrap/*");
      NodeList nodes = (NodeList)exp.evaluate(source, XPathConstants.NODESET);
      log.info("Found " + nodes.getLength() + " bootstrap nodes");
      // Each node is a bootstrap data item
      for (int i = 0; i < nodes.getLength(); i++) {
        String xml = nodeToString(nodes.item(i));
        Object msg = messageConverter.fromXML(xml);
        result.add(msg);
      }
    }
    catch (XPathExpressionException xee) {
      log.error("runOnce: Error reading config file: " + xee.toString());
    }
    catch (IOException ioe) {
      log.error("runOnce: reset fault: " + ioe.toString());
    }
    return result;
  }

  // Converts an xml node into a string that can be converted by XStream
  private String nodeToString(Node node) {
    StringWriter sw = new StringWriter();
    try {
      Transformer t = TransformerFactory.newInstance().newTransformer();
      t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
      t.setOutputProperty(OutputKeys.INDENT, "no");
      t.transform(new DOMSource(node), new StreamResult(sw));
    }
    catch (TransformerException te) {
      log.error("nodeToString Transformer Exception " + te.toString());
    }
    String result = sw.toString();
    //log.info("xml node: " + result);
    return result;
  }
}
