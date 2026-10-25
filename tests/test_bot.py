import unittest
from bot import private_text

class RoutingTests(unittest.TestCase):
    def test_private(self):
        e={'post_type':'message','message_type':'private','user_id':123,'self_id':456,'message':[{'type':'text','data':{'text':'你好'}}]}
        self.assertEqual(private_text(e), (123,'你好'))
    def test_group_and_self_ignored(self):
        self.assertIsNone(private_text({'post_type':'message','message_type':'group'}))
        self.assertIsNone(private_text({'post_type':'message','message_type':'private','user_id':1,'self_id':1}))
    def test_nontext_ignored(self):
        self.assertIsNone(private_text({'post_type':'message','message_type':'private','user_id':1,'self_id':2,'message':[{'type':'image','data':{}}]}))
