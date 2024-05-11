import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol


class rating_commodity(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(rating_commodity, self).configure_args()
        self.add_file_arg("--users-path", help="Path to the users file")

    def create_user_list(self, filename):
        user_ids = []
        with open(filename, "r") as file:
            for line in file:
                user_id = line.strip().split("\t")[0]
                user_ids.append(int(user_id))
        return user_ids

    def rating_commodity_mapper_init(self):
        users_path = self.options.users_path
        self.users = self.create_user_list(users_path)

    def rating_commodity_mapper(self, _, line):
        currentUser, item = (line.strip().split("\t"))[0].strip().split(";")
        currentUser = int(currentUser)

        for user in self.users:
            if user < currentUser:
                yield f"{user};{currentUser}", item
            elif user > currentUser:
                yield f"{currentUser};{user}", item

    def rating_commodity_reducer(self, users, items):
        user1, user2 = users.strip().split(";")
        items = list(items)
        uniqueItems = set(items)

        yield f"{user1};{user2}", f"{len(items) - len(uniqueItems)};rc"

    def steps(self):
        return [
            MRStep(
                mapper_init=self.rating_commodity_mapper_init,
                mapper=self.rating_commodity_mapper,
                reducer=self.rating_commodity_reducer,
            )
        ]


if __name__ == "__main__":
    rating_commodity().run()
