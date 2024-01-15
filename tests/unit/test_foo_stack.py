import aws_cdk as core
import aws_cdk.assertions as assertions

from foo.foo_stack import FooStack

# example tests. To run these tests, uncomment this file along with the example
# resource in foo/foo_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = FooStack(app, "foo")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
