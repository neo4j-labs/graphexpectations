from neo4j import GraphDatabase, basic_auth
from rdflib import Namespace, Literal, RDF, Graph, BNode, XSD, URIRef
import re

# Namespaces
shacl = Namespace("http://www.w3.org/ns/shacl#")
neo = Namespace("neo4j://graph.schema#")
ex = Namespace("http://ex#")



class Suite():

    def __init__(self, desc=None):
        self.__graph__ = Graph()
        self.__sets__ = []

    def add_expectations(self, sets=None):
        for s1 in sets:
            self.__sets__.append(s1)
            for t in s1._graph():
                self.__graph__.add(t)

    def bind_to_db(self, db_url=None, db_usr=None, db_pwd=None, db_name="neo4j"):
        driver = GraphDatabase.driver(
            db_url,
            auth=basic_auth(db_usr, db_pwd))

        cypher_check_n10s = '''
        show procedures yield name 
        where name starts with "n10s.validation.shacl.import.fetch" 
        return count(name) = 1 as shacl_installed 
        '''

        graph_config_present = '''
                MATCH (n:_GraphConfig) RETURN count(n) > 0 as gc_present ; 
                '''

        cypher_deploy_shapes = '''
        call n10s.validation.shacl.import.inline($payload,"Turtle") yield target 
        return count(*) as shapes_deployed ; 
        '''

        with driver.session(database=db_name) as session:
            results = session.read_transaction(
                lambda tx: tx.run(cypher_check_n10s).data())
            if (results[0]["shacl_installed"]):

                results = session.read_transaction(
                    lambda tx: tx.run(graph_config_present).data())
                gc_present = False
                if (results[0]["gc_present"]):
                    gc_present = True

                results = session.write_transaction(
                    lambda tx: tx.run(cypher_deploy_shapes,
                        payload=self.__graph__.serialize(format="turtle")).data())
                if (results[0]["shapes_deployed"]):
                    print("context successfully bound to DB")
                    return Context(driver,db_name, gc_present)
                else:
                    raise ("There was a problem deploying the expectations to the DB")
            else:
                raise("the DB does not have the n10s module installed.")


    def print_suite(self):
        print("Expectations in this Suite include " + str(len(self.__graph__)) + " triples:")
        print(self.__graph__.serialize(format="turtle"))

    def serialise(self):
        return self.__graph__.serialize(format="turtle")



class Context():

    def __init__(self, driver=None, db_name=None, gc_present=False):
        self.__db_driver___ = driver
        self.__db_name___ = db_name
        self.__gc_present___ = gc_present

    def run(self, onCollection=None):

        infix = " return focusNode as node, " if self.__gc_present___ else " match (n) where id(n) = focusNode return n as node,"

        if onCollection:
            #this is not perfect but should be good enough
            query_parts = re.split("return", onCollection, flags=re.IGNORECASE)
            as_regex = re.compile(r'\s+as\s+\S+\s*$', re.IGNORECASE)
            prefix_query = query_parts[0] + " with " + as_regex.sub("", query_parts[1], re.IGNORECASE)


            cypher_query = f'''
                    {prefix_query} as col  
                    call n10s.validation.shacl.validateSet(col) 
                    yield focusNode, nodeType, propertyShape, offendingValue, resultPath, severity, resultMessage, customMessage
                    {infix} nodeType, n10s.rdf.getIRILocalName(propertyShape) as violationType, offendingValue, 
                           resultPath as schemaElement, n10s.rdf.getIRILocalName(severity) as severity, resultMessage as comment, 
                           customMessage as msg
                    '''
        else:
            cypher_query = f'''
            call n10s.validation.shacl.validate() 
            yield focusNode, nodeType, propertyShape, offendingValue, resultPath, severity, resultMessage, customMessage
            {infix} nodeType, n10s.rdf.getIRILocalName(propertyShape) as violationType, offendingValue, 
                   resultPath as schemaElement, n10s.rdf.getIRILocalName(severity) as severity, resultMessage as comment, 
                   customMessage as msg
            '''

        with self.__db_driver___.session(database=self.__db_name___) as session:
            results = session.read_transaction(
                lambda tx: tx.run(cypher_query).data())

        return results

class Set():

    def __init__(self, nodeType=None, query=None, message=None):
        self.g = Graph()
        self.shapeId = BNode()  # URIRef(ex + nodeType )
        self.g.add((self.shapeId, RDF.type, shacl.NodeShape))
        if (nodeType):
            targetClass = URIRef(neo + nodeType)
            self.g.add((self.shapeId, shacl.targetClass, targetClass))
        elif(query):
            self.g.add((self.shapeId, shacl.targetQuery, Literal(query)))
        else:
            self.g.add((self.shapeId, shacl.targetQuery, Literal("true")))

        if(message):
            self.g.add((self.shapeId, shacl.message, Literal(message)))

    def _graph(self):
        return self.g

    def expect_property_values_to_be_between(self, property=None, minExclusive=None, minInclusive=None,
                                             maxExclusive=None, maxInclusive=None, severity=None, message=None):

        propertyId = self.__init_property_shape(URIRef(neo + property), severity, message, False)

        if (minExclusive or minInclusive):
            self.g.add((propertyId,
                        shacl.minExclusive if minExclusive else shacl.minInclusive,
                        Literal(minExclusive if minExclusive else minInclusive)))
        if (maxExclusive or maxInclusive):
            self.g.add((propertyId,
                        shacl.maxExclusive if maxExclusive else shacl.maxInclusive,
                        Literal(maxExclusive if maxExclusive else maxInclusive)))


    def expect_property_values_to_be_of_type(self, property=None, datatype=None, severity=None, message=None):
        if (datatype):
            propertyId = self.__init_property_shape(URIRef(neo + property), severity, message, False)
            self.g.add((propertyId, shacl.datatype, self.__getXSDType(datatype)))

    def expect_number_of_property_values_to_be_between(self, property=None, min=None, max=None, severity=None, message=None):

        if (property and (min or max)):
            propertyId = self.__init_property_shape(URIRef(neo + property), severity, message, False)
            if (min):
                self.g.add((propertyId, shacl.minCount, Literal(min)))
            if (max):
                self.g.add((propertyId, shacl.maxCount, Literal(max)))

    def expect_number_of_outgoing_relationship_to_be_between(self, relationship=None, min=None, max=None, severity=None, message=None):

        if (relationship and (min or max)):
            propertyId = self.__init_property_shape(URIRef(neo + relationship), severity, message, False)
            if (min):
                self.g.add((propertyId, shacl.minCount, Literal(min)))
            if (max):
                self.g.add((propertyId, shacl.maxCount, Literal(max)))


    def expect_number_of_incoming_relationship_to_be_between(self, relationship=None, min=None, max=None, severity=None, message=None):
        if (relationship and (min or max)):
            propertyId = self.__init_property_shape(URIRef(neo + relationship), severity, message,inverse=True)
            if (min):
                self.g.add((propertyId, shacl.minCount, Literal(min)))
            if (max):
                self.g.add((propertyId, shacl.maxCount, Literal(max)))


    def expect_property_values_to_be_in_set(self, property=None, valueList=None, severity=None, message=None):
        if (property and valueList):
            propertyId = self.__init_property_shape(URIRef(neo + property), severity, message, False)
            # ...because rdflib does not have a method to pass a list :(
            self.__build_rdf_list(propertyId, URIRef(shacl + "in"), valueList, True)


    def expect_property_values_to_not_be_in_set(self, property=None, valueList=None, severity=None, message=None):
        if (property and valueList):
            propertyId = self.__init_property_shape(URIRef(neo + property), severity, message, False)
            bnode = BNode()
            self.g.add((propertyId, URIRef(shacl + "in"), bnode))
            # ...because rdflib does not have a method to pass a list :(
            self.__build_rdf_list(bnode, URIRef(shacl + "not"), valueList, True)


    def expect_property_values_to_have_string_length_between(self, property=None, min=None, max=None, severity=None, message=None):
        if (property and (min or max)):
            propertyId = self.__init_property_shape(URIRef(neo + property), severity, message, False)
            if (min):
                self.g.add((propertyId, shacl.minLength, Literal(min)))
            if (max):
                self.g.add((propertyId, shacl.maxLength, Literal(max)))

    def expect_property_values_to_match_regex(self, property=None, regex=None, severity=None, message=None):
        if (property and regex):
            propertyId = self.__init_property_shape(URIRef(neo + property), severity, message, False)
            self.g.add((propertyId, shacl.pattern, Literal(regex)))

    def expect_outgoing_relationship_to_connect_to_nodes_of_type(self, relationship=None, targetType=None, severity=None, message=None):
        if (relationship and targetType):
            propertyId = self.__init_property_shape(URIRef(neo + relationship), severity, message, False)
            self.g.add((propertyId, URIRef(shacl + "class"), URIRef(neo + targetType)))

    # def expect_outgoing_relationship_to_connect_to_nodes_of_type_different_from(self, relationship=None, nodeType=None, severity=None, message=None):
    #     # not supported. Q: is it really needed?
    #     if (relationship and nodeType):
    #         propertyId = self.__init_property_shape(URIRef(neo + relationship), severity, message, False)
    #         bnode = BNode()
    #         self.g.add((propertyId, URIRef(shacl + "class"), bnode))
    #         self.g.add((bnode, URIRef(shacl + "not"), URIRef(neo + nodeType)))

    def expect_outgoing_relationship_to_connect_to_nodes_in_list(self, relationship=None, targetTypes=None, severity=None, message=None):
        if (relationship and targetTypes):
            propertyId = self.__init_property_shape(URIRef(neo + relationship), severity, message, False)
            # ...because rdflib does not have a method to pass a list :(
            self.__build_rdf_list(propertyId, URIRef(shacl + "in"), targetTypes, False)


    def expect_node_types_to_be_in_list(self, typeList=None, severity=None, message=None):
        if (typeList):
            propertyId = self.__init_property_shape(RDF.type, severity, message, False)
            # ...because rdflib does not have a method to pass a list :(
            self.__build_rdf_list(propertyId, URIRef(shacl + "in"), typeList, False)


    def expect_node_to_not_have_properties_or_rels_outside_restricted_ones(self, ignoreList=None, severity=None, message=None):
        self.g.add((self.shapeId, shacl.closed, Literal(True)))
        if (ignoreList):
            # ...because rdflib does not have a method to pass a list :(
            self.__build_rdf_list(self.shapeId, shacl.ignoredProperties, ignoreList, False)


    def expect_node_types_to_not_be_in_list(self, typeList=None, severity=None, message=None):
        if (typeList):
            propertyId = self.__init_property_shape(RDF.type, severity, message, False)
            bnode = BNode()
            self.g.add((propertyId, URIRef(shacl + "in"), bnode))
            # ...because rdflib does not have a method to pass a list :(
            self.__build_rdf_list(bnode, URIRef(shacl + "not"), typeList, False)

    def expect_outgoing_relationship_to_connect_to_nodes_not_in_list(self, relationship=None, targetTypes=None, severity=None, message=None):
        if (relationship and targetTypes):
            propertyId = self.__init_property_shape(URIRef(neo + relationship), severity, message, False)
            bnode = BNode()
            self.g.add((propertyId, URIRef(shacl + "in"), bnode))
            # ...because rdflib does not have a method to pass a list :(
            self.__build_rdf_list(bnode, URIRef(shacl + "not"), targetTypes, False)

    def expect_key_is_relationship(self, key=None, severity=None, message=None):
        if (key):
            propertyId = self.__init_property_shape(URIRef(neo + key), severity, message, False)
            self.g.add((propertyId, shacl.nodeKind, shacl.IRI))

    def expect_key_is_property(self, key=None, severity=None, message=None):
        if (key):
            propertyId = self.__init_property_shape(URIRef(neo + key), severity, message, False)
            self.g.add((propertyId, shacl.nodeKind, shacl.Literal))

    def __build_rdf_list(self, rootResource, predicate, valueList, isLiteralList):

            currentBNode = None
            list_len = len(valueList) - 1
            for value in valueList:
                if valueList.index(value) != list_len:
                    # not last
                    if (currentBNode):
                        # middle
                        newBNode = BNode()
                        self.g.add((currentBNode, RDF.rest, newBNode))
                        self.g.add((newBNode, RDF.first, Literal(value) if isLiteralList else URIRef(neo + value)))
                        currentBNode = newBNode
                    else:
                        # first
                        currentBNode = BNode()
                        self.g.add((rootResource, predicate, currentBNode))
                        self.g.add((currentBNode, RDF.first, Literal(value) if isLiteralList else URIRef(neo + value)))
                else:
                    # last
                    if not currentBNode:
                        #if it's also first (one elem list)
                        currentBNode = BNode()
                        self.g.add((rootResource, predicate, currentBNode))
                    #newBNode = BNode()
                    #self.g.add((currentBNode, RDF.rest, newBNode))
                    self.g.add((currentBNode, RDF.first , Literal(value) if isLiteralList else URIRef(neo + value)))
                    self.g.add((currentBNode, RDF.rest, RDF.nil))

    def __init_property_shape(self, targetProperty, severity, message, inverse):
        propertyId = BNode()
        self.g.add((self.shapeId, shacl.property, propertyId))
        if inverse:
            bnode = BNode()
            self.g.add((propertyId, shacl.path, bnode))
            self.g.add((bnode, shacl.inversePath, targetProperty))
        else:
            self.g.add((propertyId, shacl.path, targetProperty))
        if (severity):
            self.g.add((propertyId, shacl.severity, URIRef(shacl + severity)))
        if (message):
            self.g.add((propertyId, shacl.message, Literal(message)))
        return propertyId

    def __getXSDType(self, datatype):
        if(datatype == "string"):
            return XSD.string
        elif(datatype == "boolean"):
            return XSD.boolean
        elif (datatype == "float"):
            return XSD.float
        elif (datatype == "date"):
            return XSD.date
        elif (datatype == "datetime"):
            return XSD.datetime
        elif (datatype == "time"):
            return XSD.time
        elif (datatype == "integer"):
            return XSD.integer
        elif (datatype == "point"):
            return URIRef("http://www.opengis.net/ont/geosparql#wktLiteral")
        else:
            return XSD.string


    def print(self):
        print("#SHACL serialization of expectations in this suite contain " + str(len(self.g)) + " triples")
        return self.g.serialize(format="turtle")

    def serialise(self):
        return self.g.serialize(format="turtle")