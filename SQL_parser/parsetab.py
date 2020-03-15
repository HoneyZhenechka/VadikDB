
# parsetab.py
# This file is automatically generated. Do not edit.
# pylint: disable=W,C,R
_tabversion = '3.10'

_lr_method = 'LALR'

_lr_signature = 'ALL AND COMMA CREATE DELETE DIVISION DOT DROP ENDREQUEST EQUAL FROM GREATER_THAN GREATER_THAN_OR_EQUAL INSERT INTERSECT INTO JOIN LBRACKET LEFT LESS_THAN LESS_THAN_OR_EQUAL MINUS NAME NOT NOT_EQUAL ON OR OUTER PLUS QUOTE RBRACKET RIGHT SELECT SET SHOW STAR TABLE UNION UPDATE USING VALUES WHERE bol bool float int strstart : create ENDREQUEST\n             | show ENDREQUEST\n             | drop ENDREQUEST\n             | tree_selects ENDREQUEST\n             | insert ENDREQUEST\n             | update ENDREQUEST\n             | delete ENDREQUESTcreate : CREATE create_bodycreate_body : TABLE NAME LBRACKET variables RBRACKETvariables : NAME type\n              | variables COMMA NAME typeshow : SHOW CREATE TABLE NAMEdrop : DROP TABLE NAMEjoin : JOIN\n            | LEFT OUTER JOIN\n            | RIGHT OUTER JOINjoin_right_table : NAME ON field EQUAL field\n            | NAME USING LBRACKET field RBRACKETunion : UNION\n            | UNION ALLintersect : INTERSECTtree_selects : nested_selectsnested_selects : select\n                    | select union nested_selects\n                    | select intersect nested_selectsselect : SELECT select_body join join_right_table\n              | SELECT select_body join join_right_table condition\n              | SELECT select_body\n              | SELECT select_body conditionselect_body : fields FROM NAME\n                   | STAR COMMA fields FROM NAME\n                   | STAR FROM NAMEinsert : INSERT insert_bodyinsert_body : INTO NAME VALUES LBRACKET values RBRACKET\n                   | INTO NAME LBRACKET fields RBRACKET VALUES LBRACKET values RBRACKETupdate : UPDATE update_bodyupdate_body : NAME SET expression\n                   | NAME SET expression conditionexpression : field EQUAL tree_expression\n                  | expression COMMA field EQUAL tree_expressiondelete : DELETE FROM NAME\n              | DELETE FROM NAME conditionvalues : value\n              | values COMMA valuevalue : NAME\n             | QUOTE NAME QUOTEfields : field\n              | fields COMMA fieldfield : NAME\n            | NAME DOT NAMEcondition : WHERE tree_conditiontree_condition : tree_comparison operator_condition tree_condition\n                        | tree_comparisontree_comparison :  tree_expression operator_comparison tree_expressiontree_expression : value\n            | value operator_expression tree_expression\n            | operator_expression tree_expression\n            | LBRACKET tree_expression RBRACKET\n            | tree_expression operator_expression tree_expressionoperator_condition : AND\n                            | OR operator_comparison : EQUAL\n                            | NOT_EQUAL\n                            | GREATER_THAN\n                            | LESS_THAN\n                            | GREATER_THAN_OR_EQUAL\n                            | LESS_THAN_OR_EQUALoperator_expression : PLUS\n                | MINUS\n                | STAR\n                | DIVISIONtype : int\n            | str\n            | bol\n            | bool\n            | float'
    
_lr_action_items = {'CREATE':([0,10,],[9,27,]),'SHOW':([0,],[10,]),'DROP':([0,],[11,]),'INSERT':([0,],[13,]),'UPDATE':([0,],[14,]),'DELETE':([0,],[15,]),'SELECT':([0,34,35,36,37,51,],[17,17,17,-19,-21,-20,]),'$end':([1,18,19,20,21,22,23,24,],[0,-1,-2,-3,-4,-5,-6,-7,]),'ENDREQUEST':([2,3,4,5,6,7,8,12,16,25,29,31,38,40,45,48,49,50,53,64,67,69,70,74,75,77,80,86,88,90,95,98,115,125,131,134,135,136,137,138,139,140,142,151,152,153,155,],[18,19,20,21,22,23,24,-22,-23,-8,-33,-36,-28,-49,-13,-41,-24,-25,-29,-12,-37,-42,-26,-51,-53,-55,-45,-30,-50,-32,-38,-27,-57,-9,-39,-52,-54,-59,-56,-58,-46,-31,-34,-40,-17,-18,-35,]),'TABLE':([9,11,27,],[26,28,44,]),'INTO':([13,],[30,]),'NAME':([14,17,26,28,30,33,44,47,52,54,57,58,59,60,61,62,63,66,78,79,81,82,83,84,85,93,96,97,99,101,102,103,104,105,106,107,108,109,110,111,112,113,114,118,126,133,143,145,146,150,],[32,40,43,45,46,48,64,40,71,-14,80,86,40,88,40,90,91,40,80,80,117,-68,-69,-70,-71,80,40,80,40,-15,-16,80,-60,-61,80,80,-62,-63,-64,-65,-66,-67,80,140,141,40,80,80,40,80,]),'FROM':([15,39,40,41,42,87,88,89,],[33,58,-49,62,-47,-48,-50,118,]),'UNION':([16,38,40,53,70,74,75,77,80,86,88,90,98,115,134,135,136,137,138,139,140,152,153,],[36,-28,-49,-29,-26,-51,-53,-55,-45,-30,-50,-32,-27,-57,-52,-54,-59,-56,-58,-46,-31,-17,-18,]),'INTERSECT':([16,38,40,53,70,74,75,77,80,86,88,90,98,115,134,135,136,137,138,139,140,152,153,],[37,-28,-49,-29,-26,-51,-53,-55,-45,-30,-50,-32,-27,-57,-52,-54,-59,-56,-58,-46,-31,-17,-18,]),'STAR':([17,57,76,77,78,79,80,82,83,84,85,97,103,104,105,106,107,108,109,110,111,112,113,114,115,116,131,135,136,137,138,139,145,151,],[41,84,84,84,84,84,-45,-68,-69,-70,-71,84,84,-60,-61,84,84,-62,-63,-64,-65,-66,-67,84,84,84,84,84,84,84,-58,-46,84,84,]),'SET':([32,],[47,]),'ALL':([36,],[51,]),'JOIN':([38,72,73,86,90,140,],[54,101,102,-30,-32,-31,]),'LEFT':([38,86,90,140,],[55,-30,-32,-31,]),'RIGHT':([38,86,90,140,],[56,-30,-32,-31,]),'WHERE':([38,40,48,67,70,77,80,86,88,90,115,131,136,137,138,139,140,151,152,153,],[57,-49,57,57,57,-55,-45,-30,-50,-32,-57,-39,-59,-56,-58,-46,-31,-40,-17,-18,]),'COMMA':([39,40,41,42,67,77,80,87,88,89,92,94,115,119,120,121,122,123,124,127,128,131,136,137,138,139,148,149,151,154,],[59,-49,61,-47,96,-55,-45,-48,-50,59,126,59,-57,-10,-72,-73,-74,-75,-76,143,-43,-39,-59,-56,-58,-46,-11,-44,-40,143,]),'EQUAL':([40,68,76,77,80,88,115,130,132,136,137,138,139,],[-49,97,108,-55,-45,-50,-57,145,146,-59,-56,-58,-46,]),'RBRACKET':([40,42,77,80,87,88,92,94,115,116,119,120,121,122,123,124,127,128,136,137,138,139,147,148,149,154,],[-49,-47,-55,-45,-48,-50,125,129,-57,138,-10,-72,-73,-74,-75,-76,142,-43,-59,-56,-58,-46,153,-11,-44,155,]),'DOT':([40,],[60,]),'LBRACKET':([43,46,57,65,78,79,82,83,84,85,97,100,103,104,105,106,107,108,109,110,111,112,113,114,144,145,],[63,66,79,93,79,79,-68,-69,-70,-71,79,133,79,-60,-61,79,79,-62,-63,-64,-65,-66,-67,79,150,79,]),'VALUES':([46,129,],[65,144,]),'OUTER':([55,56,],[72,73,]),'QUOTE':([57,78,79,82,83,84,85,93,97,103,104,105,106,107,108,109,110,111,112,113,114,117,143,145,150,],[81,81,81,-68,-69,-70,-71,81,81,81,-60,-61,81,81,-62,-63,-64,-65,-66,-67,81,139,81,81,81,]),'PLUS':([57,76,77,78,79,80,82,83,84,85,97,103,104,105,106,107,108,109,110,111,112,113,114,115,116,131,135,136,137,138,139,145,151,],[82,82,82,82,82,-45,-68,-69,-70,-71,82,82,-60,-61,82,82,-62,-63,-64,-65,-66,-67,82,82,82,82,82,82,82,-58,-46,82,82,]),'MINUS':([57,76,77,78,79,80,82,83,84,85,97,103,104,105,106,107,108,109,110,111,112,113,114,115,116,131,135,136,137,138,139,145,151,],[83,83,83,83,83,-45,-68,-69,-70,-71,83,83,-60,-61,83,83,-62,-63,-64,-65,-66,-67,83,83,83,83,83,83,83,-58,-46,83,83,]),'DIVISION':([57,76,77,78,79,80,82,83,84,85,97,103,104,105,106,107,108,109,110,111,112,113,114,115,116,131,135,136,137,138,139,145,151,],[85,85,85,85,85,-45,-68,-69,-70,-71,85,85,-60,-61,85,85,-62,-63,-64,-65,-66,-67,85,85,85,85,85,85,85,-58,-46,85,85,]),'ON':([71,],[99,]),'USING':([71,],[100,]),'AND':([75,77,80,115,135,136,137,138,139,],[104,-55,-45,-57,-54,-59,-56,-58,-46,]),'OR':([75,77,80,115,135,136,137,138,139,],[105,-55,-45,-57,-54,-59,-56,-58,-46,]),'NOT_EQUAL':([76,77,80,115,136,137,138,139,],[109,-55,-45,-57,-59,-56,-58,-46,]),'GREATER_THAN':([76,77,80,115,136,137,138,139,],[110,-55,-45,-57,-59,-56,-58,-46,]),'LESS_THAN':([76,77,80,115,136,137,138,139,],[111,-55,-45,-57,-59,-56,-58,-46,]),'GREATER_THAN_OR_EQUAL':([76,77,80,115,136,137,138,139,],[112,-55,-45,-57,-59,-56,-58,-46,]),'LESS_THAN_OR_EQUAL':([76,77,80,115,136,137,138,139,],[113,-55,-45,-57,-59,-56,-58,-46,]),'int':([91,141,],[120,120,]),'str':([91,141,],[121,121,]),'bol':([91,141,],[122,122,]),'bool':([91,141,],[123,123,]),'float':([91,141,],[124,124,]),}

_lr_action = {}
for _k, _v in _lr_action_items.items():
   for _x,_y in zip(_v[0],_v[1]):
      if not _x in _lr_action:  _lr_action[_x] = {}
      _lr_action[_x][_k] = _y
del _lr_action_items

_lr_goto_items = {'start':([0,],[1,]),'create':([0,],[2,]),'show':([0,],[3,]),'drop':([0,],[4,]),'tree_selects':([0,],[5,]),'insert':([0,],[6,]),'update':([0,],[7,]),'delete':([0,],[8,]),'nested_selects':([0,34,35,],[12,49,50,]),'select':([0,34,35,],[16,16,16,]),'create_body':([9,],[25,]),'insert_body':([13,],[29,]),'update_body':([14,],[31,]),'union':([16,],[34,]),'intersect':([16,],[35,]),'select_body':([17,],[38,]),'fields':([17,61,66,],[39,89,94,]),'field':([17,47,59,61,66,96,99,133,146,],[42,68,87,42,42,130,132,147,152,]),'join':([38,],[52,]),'condition':([38,48,67,70,],[53,69,95,98,]),'expression':([47,],[67,]),'join_right_table':([52,],[70,]),'tree_condition':([57,103,],[74,134,]),'tree_comparison':([57,103,],[75,75,]),'tree_expression':([57,78,79,97,103,106,107,114,145,],[76,115,116,131,76,135,136,137,151,]),'value':([57,78,79,93,97,103,106,107,114,143,145,150,],[77,77,77,128,77,77,77,77,77,149,77,128,]),'operator_expression':([57,76,77,78,79,97,103,106,107,114,115,116,131,135,136,137,145,151,],[78,107,114,78,78,78,78,78,78,78,107,107,107,107,107,107,78,107,]),'variables':([63,],[92,]),'operator_condition':([75,],[103,]),'operator_comparison':([76,],[106,]),'type':([91,141,],[119,148,]),'values':([93,150,],[127,154,]),}

_lr_goto = {}
for _k, _v in _lr_goto_items.items():
   for _x, _y in zip(_v[0], _v[1]):
       if not _x in _lr_goto: _lr_goto[_x] = {}
       _lr_goto[_x][_k] = _y
del _lr_goto_items
_lr_productions = [
  ("S' -> start","S'",1,None,None,None),
  ('start -> create ENDREQUEST','start',2,'p_start','SQL_parser.py',271),
  ('start -> show ENDREQUEST','start',2,'p_start','SQL_parser.py',272),
  ('start -> drop ENDREQUEST','start',2,'p_start','SQL_parser.py',273),
  ('start -> tree_selects ENDREQUEST','start',2,'p_start','SQL_parser.py',274),
  ('start -> insert ENDREQUEST','start',2,'p_start','SQL_parser.py',275),
  ('start -> update ENDREQUEST','start',2,'p_start','SQL_parser.py',276),
  ('start -> delete ENDREQUEST','start',2,'p_start','SQL_parser.py',277),
  ('create -> CREATE create_body','create',2,'p_create','SQL_parser.py',283),
  ('create_body -> TABLE NAME LBRACKET variables RBRACKET','create_body',5,'p_create_body','SQL_parser.py',289),
  ('variables -> NAME type','variables',2,'p_variables','SQL_parser.py',295),
  ('variables -> variables COMMA NAME type','variables',4,'p_variables','SQL_parser.py',296),
  ('show -> SHOW CREATE TABLE NAME','show',4,'p_show','SQL_parser.py',307),
  ('drop -> DROP TABLE NAME','drop',3,'p_drop','SQL_parser.py',313),
  ('join -> JOIN','join',1,'p_join','SQL_parser.py',319),
  ('join -> LEFT OUTER JOIN','join',3,'p_join','SQL_parser.py',320),
  ('join -> RIGHT OUTER JOIN','join',3,'p_join','SQL_parser.py',321),
  ('join_right_table -> NAME ON field EQUAL field','join_right_table',5,'p_join_right_table','SQL_parser.py',330),
  ('join_right_table -> NAME USING LBRACKET field RBRACKET','join_right_table',5,'p_join_right_table','SQL_parser.py',331),
  ('union -> UNION','union',1,'p_union','SQL_parser.py',339),
  ('union -> UNION ALL','union',2,'p_union','SQL_parser.py',340),
  ('intersect -> INTERSECT','intersect',1,'p_intersect','SQL_parser.py',349),
  ('tree_selects -> nested_selects','tree_selects',1,'p_tree_selects','SQL_parser.py',355),
  ('nested_selects -> select','nested_selects',1,'p_nested_selects','SQL_parser.py',361),
  ('nested_selects -> select union nested_selects','nested_selects',3,'p_nested_selects','SQL_parser.py',362),
  ('nested_selects -> select intersect nested_selects','nested_selects',3,'p_nested_selects','SQL_parser.py',363),
  ('select -> SELECT select_body join join_right_table','select',4,'p_select','SQL_parser.py',375),
  ('select -> SELECT select_body join join_right_table condition','select',5,'p_select','SQL_parser.py',376),
  ('select -> SELECT select_body','select',2,'p_select','SQL_parser.py',377),
  ('select -> SELECT select_body condition','select',3,'p_select','SQL_parser.py',378),
  ('select_body -> fields FROM NAME','select_body',3,'p_select_body','SQL_parser.py',391),
  ('select_body -> STAR COMMA fields FROM NAME','select_body',5,'p_select_body','SQL_parser.py',392),
  ('select_body -> STAR FROM NAME','select_body',3,'p_select_body','SQL_parser.py',393),
  ('insert -> INSERT insert_body','insert',2,'p_insert','SQL_parser.py',404),
  ('insert_body -> INTO NAME VALUES LBRACKET values RBRACKET','insert_body',6,'p_insert_body','SQL_parser.py',410),
  ('insert_body -> INTO NAME LBRACKET fields RBRACKET VALUES LBRACKET values RBRACKET','insert_body',9,'p_insert_body','SQL_parser.py',411),
  ('update -> UPDATE update_body','update',2,'p_update','SQL_parser.py',420),
  ('update_body -> NAME SET expression','update_body',3,'p_update_body','SQL_parser.py',426),
  ('update_body -> NAME SET expression condition','update_body',4,'p_update_body','SQL_parser.py',427),
  ('expression -> field EQUAL tree_expression','expression',3,'p_expression','SQL_parser.py',436),
  ('expression -> expression COMMA field EQUAL tree_expression','expression',5,'p_expression','SQL_parser.py',437),
  ('delete -> DELETE FROM NAME','delete',3,'p_delete','SQL_parser.py',450),
  ('delete -> DELETE FROM NAME condition','delete',4,'p_delete','SQL_parser.py',451),
  ('values -> value','values',1,'p_values','SQL_parser.py',459),
  ('values -> values COMMA value','values',3,'p_values','SQL_parser.py',460),
  ('value -> NAME','value',1,'p_value','SQL_parser.py',471),
  ('value -> QUOTE NAME QUOTE','value',3,'p_value','SQL_parser.py',472),
  ('fields -> field','fields',1,'p_fields','SQL_parser.py',481),
  ('fields -> fields COMMA field','fields',3,'p_fields','SQL_parser.py',482),
  ('field -> NAME','field',1,'p_field','SQL_parser.py',493),
  ('field -> NAME DOT NAME','field',3,'p_field','SQL_parser.py',494),
  ('condition -> WHERE tree_condition','condition',2,'p_condition','SQL_parser.py',503),
  ('tree_condition -> tree_comparison operator_condition tree_condition','tree_condition',3,'p_tree_condition','SQL_parser.py',509),
  ('tree_condition -> tree_comparison','tree_condition',1,'p_tree_condition','SQL_parser.py',510),
  ('tree_comparison -> tree_expression operator_comparison tree_expression','tree_comparison',3,'p_tree_comparison','SQL_parser.py',526),
  ('tree_expression -> value','tree_expression',1,'p_tree_expression','SQL_parser.py',535),
  ('tree_expression -> value operator_expression tree_expression','tree_expression',3,'p_tree_expression','SQL_parser.py',536),
  ('tree_expression -> operator_expression tree_expression','tree_expression',2,'p_tree_expression','SQL_parser.py',537),
  ('tree_expression -> LBRACKET tree_expression RBRACKET','tree_expression',3,'p_tree_expression','SQL_parser.py',538),
  ('tree_expression -> tree_expression operator_expression tree_expression','tree_expression',3,'p_tree_expression','SQL_parser.py',539),
  ('operator_condition -> AND','operator_condition',1,'p_operator_condition','SQL_parser.py',553),
  ('operator_condition -> OR','operator_condition',1,'p_operator_condition','SQL_parser.py',554),
  ('operator_comparison -> EQUAL','operator_comparison',1,'p_operator_comparison','SQL_parser.py',560),
  ('operator_comparison -> NOT_EQUAL','operator_comparison',1,'p_operator_comparison','SQL_parser.py',561),
  ('operator_comparison -> GREATER_THAN','operator_comparison',1,'p_operator_comparison','SQL_parser.py',562),
  ('operator_comparison -> LESS_THAN','operator_comparison',1,'p_operator_comparison','SQL_parser.py',563),
  ('operator_comparison -> GREATER_THAN_OR_EQUAL','operator_comparison',1,'p_operator_comparison','SQL_parser.py',564),
  ('operator_comparison -> LESS_THAN_OR_EQUAL','operator_comparison',1,'p_operator_comparison','SQL_parser.py',565),
  ('operator_expression -> PLUS','operator_expression',1,'p_operator_expression','SQL_parser.py',571),
  ('operator_expression -> MINUS','operator_expression',1,'p_operator_expression','SQL_parser.py',572),
  ('operator_expression -> STAR','operator_expression',1,'p_operator_expression','SQL_parser.py',573),
  ('operator_expression -> DIVISION','operator_expression',1,'p_operator_expression','SQL_parser.py',574),
  ('type -> int','type',1,'p_type','SQL_parser.py',580),
  ('type -> str','type',1,'p_type','SQL_parser.py',581),
  ('type -> bol','type',1,'p_type','SQL_parser.py',582),
  ('type -> bool','type',1,'p_type','SQL_parser.py',583),
  ('type -> float','type',1,'p_type','SQL_parser.py',584),
]
